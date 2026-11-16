// Copyright 2026 litep2p developers
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use crate::{
    protocol::{InnerTransportEvent, SubstreamKeepAlive},
    transport::manager::{InboundProtocol, ProtocolContext},
    types::protocol::ProtocolName,
};

use parking_lot::RwLock;
use tokio::sync::{mpsc::Sender, watch};

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

/// Installed protocols shared between the transport manager and the open connections.
#[derive(Debug, Clone, Default)]
pub struct ProtocolRegistry(Arc<RwLock<Protocols>>);

/// [`ProtocolRegistry`] that doesn't keep the senders of the installed protocols alive.
#[derive(Debug, Clone, Default)]
pub struct WeakProtocolRegistry(Weak<RwLock<Protocols>>);

impl WeakProtocolRegistry {
    pub fn upgrade(&self) -> Option<ProtocolRegistry> {
        self.0.upgrade().map(ProtocolRegistry)
    }
}

#[derive(Debug)]
struct Protocols {
    contexts: HashMap<ProtocolName, ProtocolContext>,
    /// Bumped on every change, so that open connections can tell the protocols registered after
    /// them.
    changes: watch::Sender<u64>,
    /// The value last sent on `changes`, readable without the `watch` lock.
    generation: u64,
    /// Maps a fallback name to the main protocol name.
    fallback_names: HashMap<ProtocolName, ProtocolName>,
    keep_alives: Arc<HashMap<ProtocolName, SubstreamKeepAlive>>,
}

/// The maps derived from the installed protocols.
type Derived = (
    HashMap<ProtocolName, ProtocolName>,
    Arc<HashMap<ProtocolName, SubstreamKeepAlive>>,
);

impl Protocols {
    fn new(contexts: HashMap<ProtocolName, ProtocolContext>) -> Self {
        let (fallback_names, keep_alives) = Self::derive(&contexts);

        Self {
            contexts,
            changes: watch::channel(0).0,
            generation: 0,
            fallback_names,
            keep_alives,
        }
    }

    /// Build the fallback-name and keep-alive maps of `contexts`.
    fn derive(contexts: &HashMap<ProtocolName, ProtocolContext>) -> Derived {
        let fallback_names = contexts
            .iter()
            .flat_map(|(protocol, context)| {
                context
                    .fallback_names
                    .iter()
                    .map(|fallback| (fallback.clone(), protocol.clone()))
            })
            .collect();

        let keep_alives = contexts
            .iter()
            .filter(|(_, context)| context.inbound == InboundProtocol::Accept)
            .flat_map(|(protocol, context)| {
                std::iter::once(protocol)
                    .chain(&context.fallback_names)
                    .map(|name| (name.clone(), context.keep_alive))
            })
            .collect();

        (fallback_names, Arc::new(keep_alives))
    }
}

impl Default for Protocols {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

impl From<HashMap<ProtocolName, ProtocolContext>> for ProtocolRegistry {
    fn from(protocols: HashMap<ProtocolName, ProtocolContext>) -> Self {
        Self(Arc::new(RwLock::new(Protocols::new(protocols))))
    }
}

impl ProtocolRegistry {
    pub fn downgrade(&self) -> WeakProtocolRegistry {
        WeakProtocolRegistry(Arc::downgrade(&self.0))
    }

    fn update<R>(&self, f: impl FnOnce(&mut HashMap<ProtocolName, ProtocolContext>) -> R) -> R {
        let mut protocols = self.0.write();
        let mut contexts = std::mem::take(&mut protocols.contexts);
        let updated = f(&mut contexts);
        (protocols.fallback_names, protocols.keep_alives) = Protocols::derive(&contexts);
        protocols.contexts = contexts;
        protocols.generation += 1;
        let generation = protocols.generation;
        protocols.changes.send_replace(generation);
        updated
    }

    /// Number of changes to the installed protocols so far.
    pub fn generation(&self) -> u64 {
        self.0.read().generation
    }

    /// Changes to the installed protocols, from now on.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.0.read().changes.subscribe()
    }

    pub fn insert(&self, protocol: ProtocolName, context: ProtocolContext) {
        self.update(|contexts| contexts.insert(protocol, context));
    }

    /// Remove `protocol` if it's still installed with `tx`.
    pub fn remove(&self, protocol: &ProtocolName, tx: &Sender<InnerTransportEvent>) -> bool {
        self.update(|contexts| {
            contexts.get(protocol).is_some_and(|context| context.tx.same_channel(tx))
                && contexts.remove(protocol).is_some()
        })
    }

    /// Check if `name` is the main or a fallback name of an installed protocol.
    pub fn contains(&self, name: &ProtocolName) -> bool {
        self.get(name).is_some()
    }

    /// Get the main name and the context of the protocol that `name` belongs to.
    pub fn get(&self, name: &ProtocolName) -> Option<(ProtocolName, ProtocolContext)> {
        self.get_with_generation(name).0
    }

    /// [`Self::get`], with the [`Self::generation`] it was read at, under one lock.
    pub fn get_with_generation(
        &self,
        name: &ProtocolName,
    ) -> (Option<(ProtocolName, ProtocolContext)>, u64) {
        let protocols = self.0.read();
        let found = protocols
            .contexts
            .get_key_value(protocols.fallback_names.get(name).unwrap_or(name))
            .map(|(protocol, context)| (protocol.clone(), context.clone()));

        (found, protocols.generation)
    }

    /// Main names of the protocols advertised to remote peers.
    pub fn advertised(&self) -> Vec<ProtocolName> {
        self.0
            .read()
            .contexts
            .iter()
            .filter(|(_, context)| context.inbound == InboundProtocol::Accept)
            .map(|(protocol, _)| protocol.clone())
            .collect()
    }

    /// Keep-alive settings of the main and fallback names that remote peers can negotiate.
    pub fn keep_alives(&self) -> Arc<HashMap<ProtocolName, SubstreamKeepAlive>> {
        self.0.read().keep_alives.clone()
    }

    pub fn senders(&self) -> Vec<(ProtocolName, Sender<InnerTransportEvent>)> {
        self.0
            .read()
            .contexts
            .iter()
            .map(|(protocol, context)| (protocol.clone(), context.tx.clone()))
            .collect()
    }
}
