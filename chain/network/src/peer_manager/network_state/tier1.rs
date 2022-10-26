use crate::accounts_data;
use crate::config;
use crate::network_protocol::{
    AccountData, PeerAddr, PeerInfo, PeerMessage, SignedAccountData, SyncAccountsData,
};
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::connection;
use crate::tcp;
use crate::time;
use crate::types::PeerType;
use near_o11y::log_assert;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use rand::seq::IteratorRandom as _;
use rand::seq::SliceRandom as _;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl super::NetworkState {
    // Returns ValidatorConfig of this node iff it belongs to TIER1 according to `accounts_data`.
    pub fn tier1_validator_config(
        &self,
        accounts_data: &accounts_data::CacheSnapshot,
    ) -> Option<&config::ValidatorConfig> {
        if self.config.features.tier1.is_none() {
            return None;
        }
        self.config.validator.as_ref().filter(|cfg| {
            accounts_data.contains_account_key(cfg.signer.validator_id(), &cfg.signer.public_key())
        })
    }

    /// Tries to connect to ALL trusted proxies from the config, then broadcasts AccountData with
    /// the set of proxies it managed to connect to. This way other TIER1 nodes can just connect
    /// to ANY proxy of this node.
    pub async fn tier1_advertise_proxies(
        self: &Arc<Self>,
        clock: &time::Clock,
    ) -> Vec<Arc<SignedAccountData>> {
        let accounts_data = self.accounts_data.load();
        let tier1 = self.tier1.load();
        let vc = match self.tier1_validator_config(&accounts_data) {
            Some(it) => it,
            None => return vec![],
        };
        let proxies = match &vc.proxies {
            config::ValidatorProxies::Dynamic(_) => {
                // TODO(gprusak): If Dynamic are specified,
                // it means that this node is its own proxy.
                // Resolve the public IP of this node using those STUN servers,
                // then connect to yourself (to verify the public IP).
                vec![]
            }
            config::ValidatorProxies::Static(peer_addrs) => peer_addrs.clone(),
        };
        tracing::debug!(target:"test","proxies = {proxies:?}");
        // Try to connect to all proxies in parallel.
        let mut handles = vec![];
        for proxy in proxies {
            // Skip the proxies we are already connected to.
            if tier1.ready.contains_key(&proxy.peer_id) {
                continue;
            }
            handles.push(async move {
                let stream = tcp::Stream::connect(
                    &PeerInfo {
                        id: proxy.peer_id.clone(),
                        addr: Some(proxy.addr),
                        account_id: None,
                    },
                    tcp::Tier::T1,
                )
                .await?;
                tracing::debug!(target:"test","spawning connection to {proxy:?}");
                anyhow::Ok(
                    PeerActor::spawn_and_handshake(clock.clone(), stream, None, self.clone())
                        .await?,
                )
            });
        }
        for res in futures_util::future::join_all(handles).await {
            if let Err(err) = res {
                tracing::info!(target:"network", ?err, "failed to establish a TIER1 proxy");
            }
        }

        // Snapshot tier1 connections again before broadcasting.
        let tier1 = self.tier1.load();

        let my_proxies = match &vc.proxies {
            config::ValidatorProxies::Dynamic(_) => match tier1.ready.get(&self.config.node_id()) {
                Some(conn) => {
                    log_assert!(PeerType::Outbound == conn.peer_type);
                    log_assert!(conn.peer_info.addr.is_some());
                    match conn.peer_info.addr {
                        Some(addr) => vec![PeerAddr { peer_id: self.config.node_id(), addr }],
                        None => vec![],
                    }
                }
                None => vec![],
            },
            config::ValidatorProxies::Static(proxies) => {
                let mut connected_proxies = vec![];
                for proxy in proxies {
                    match tier1.ready.get(&proxy.peer_id) {
                        // Here we compare the address from the config with the
                        // address of the connection (which is the IP, to which the
                        // TCP socket is connected + port indicated by the peer).
                        // We will broadcast only those addresses which we confirmed are
                        // valid (i.e. we managed to connect to them).
                        //
                        // TODO(gprusak): It may happen that a single peer will be
                        // available under multiple IPs, in which case, we should
                        // prefer to connect to the IP from the config, however
                        // that would require having separate inbound and outbound
                        // pools, so that both endpoints can keep a connection
                        // to the IP that they prefer. This is a corner case which can happen
                        // only if 2 TIER1 validators are proxies for some other validator.
                        Some(conn) if conn.peer_info.addr == Some(proxy.addr) => {
                            connected_proxies.push(proxy.clone());
                        }
                        _ => {}
                    }
                }
                connected_proxies
            }
        };
        let now = clock.now_utc();
        let my_data = self
            .accounts_data
            .load()
            .epochs(&vc.signer.validator_id(), &vc.signer.public_key())
            .iter()
            .map(|epoch_id| {
                // This unwrap is safe, because we did signed a sample payload during
                // config validation. See config::Config::new().
                Arc::new(
                    AccountData {
                        peer_id: Some(self.config.node_id()),
                        epoch_id: epoch_id.clone(),
                        account_id: vc.signer.validator_id().clone(),
                        timestamp: now,
                        peers: my_proxies.clone(),
                    }
                    .sign(vc.signer.as_ref())
                    .unwrap(),
                )
            })
            .collect();
        let (new_data, err) = self.accounts_data.insert(my_data).await;
        // Inserting node's own AccountData should never fail.
        if let Some(err) = err {
            panic!("inserting node's own AccountData to self.state.accounts_data: {err}");
        }
        if new_data.is_empty() {
            // If new_data is empty, it means that accounts_data contains entry newer than `now`.
            // This means that the UTC clock went backwards since the last broadcast.
            // TODO(gprusak): UTC timestamp acts just as a "AccountsData version ID", so perhaps
            // it would be semantically better to use "last timestamp + eps" as a fallback.
            tracing::warn!("cannot broadcast TIER1 proxy addresses: UTC clock went backwards");
            return vec![];
        }
        self.tier2.broadcast_message(Arc::new(PeerMessage::SyncAccountsData(SyncAccountsData {
            incremental: true,
            requesting_full_sync: false,
            accounts_data: new_data.clone(),
        })));
        new_data
    }

    /// Closes TIER1 connections from nodes which are not TIER1 any more.
    /// If this node is TIER1, it additionally connects to proxies of other TIER1 nodes.
    pub async fn tier1_connect(self: &Arc<Self>, clock: &time::Clock) {
        let tier1_cfg = match &self.config.features.tier1 {
            Some(it) => it,
            None => return,
        };
        if !tier1_cfg.enable_outbound {
            return;
        }
        let accounts_data = self.accounts_data.load();
        let validator_cfg = self.tier1_validator_config(&accounts_data);

        // Construct indices on accounts_data.
        let mut accounts_by_proxy = HashMap::<_, Vec<_>>::new();
        let mut proxies_by_account = HashMap::<_, Vec<_>>::new();
        for d in accounts_data.data.values() {
            proxies_by_account.entry(&d.account_id).or_default().extend(d.peers.iter());
            for p in &d.peers {
                accounts_by_proxy.entry(&p.peer_id).or_default().push(&d.account_id);
            }
        }

        // Browse the connections from newest to oldest.
        let tier1 = self.tier1.load();
        let mut ready: Vec<_> = tier1.ready.values().collect();
        ready.sort_unstable_by_key(|c| c.established_time);
        ready.reverse();

        // Select the oldest TIER1 connection for each account.
        let mut safe = HashMap::<&AccountId, &PeerId>::new();
        if validator_cfg.is_some() {
            // TIER1 nodes can also connect to TIER1 proxies.
            for conn in &ready {
                let peer_id = &conn.peer_info.id;
                for account_id in accounts_by_proxy.get(peer_id).into_iter().flatten() {
                    safe.insert(account_id, peer_id);
                }
            }
        }
        // Direct TIER1 connections have priority.
        for ((_, account_id), key) in accounts_data.keys.iter() {
            if let Some(conn) = tier1.ready_by_account_key.get(&key) {
                safe.insert(account_id, &conn.peer_info.id);
            }
        }

        // Construct a safe set of connections.
        let mut safe_set: HashSet<PeerId> = safe.values().map(|v| (*v).clone()).collect();
        // Add proxies of our node to the safe set.
        if let Some(vc) = validator_cfg {
            match &vc.proxies {
                config::ValidatorProxies::Dynamic(_) => {
                    safe_set.insert(self.config.node_id());
                }
                config::ValidatorProxies::Static(peer_addrs) => {
                    // TODO(gprusak): here we add peer_id to a safe set, even if
                    // the conn.peer_addr doesn't match the address from the validator config
                    // (so we cannot advertise it as our proxy). Consider making it more precise.
                    safe_set.extend(peer_addrs.iter().map(|pa| pa.peer_id.clone()));
                }
            }
        }
        // Close all other connections, as they are redundant or are no longer TIER1.
        for conn in tier1.ready.values() {
            if !safe_set.contains(&conn.peer_info.id) {
                conn.stop(None);
            }
        }
        if let Some(vc) = validator_cfg {
            // Try to establish new TIER1 connections to accounts in random order.
            let mut handles = vec![];
            let mut account_ids: Vec<_> = proxies_by_account.keys().copied().collect();
            account_ids.shuffle(&mut rand::thread_rng());
            for account_id in account_ids {
                // tier1_establish_proxies() is responsible for connecting to proxies
                // of this node. tier1_establish_connections() connects only to proxies
                // of other TIER1 nodes.
                if account_id == vc.signer.validator_id() {
                    continue;
                }
                // Bound the number of connections established at a single call to
                // tier1_establish_connections().
                if handles.len() >= tier1_cfg.new_connections_per_attempt {
                    break;
                }
                // If we are already connected to some proxy of account_id, then
                // don't establish another connection.
                if safe.contains_key(account_id) {
                    continue;
                }
                // Find addresses of proxies of account_id.
                let proxies: Vec<&PeerAddr> =
                    proxies_by_account.get(account_id).into_iter().flatten().map(|x| *x).collect();
                // Select a random proxy of the account_id and try to connect to it.
                let proxy = proxies.iter().choose(&mut rand::thread_rng());
                if let Some(proxy) = proxy {
                    let proxy = (*proxy).clone();
                    handles.push(async move {
                        let stream = tcp::Stream::connect(
                            &PeerInfo {
                                id: proxy.peer_id,
                                addr: Some(proxy.addr),
                                account_id: None,
                            },
                            tcp::Tier::T1,
                        )
                        .await?;
                        PeerActor::spawn_and_handshake(clock.clone(), stream, None, self.clone())
                            .await
                    });
                }
            }
            tracing::debug!(target:"network","{}: establishing {} new connections",self.config.node_id(),handles.len());
            for res in futures_util::future::join_all(handles).await {
                if let Err(err) = res {
                    tracing::info!(target:"network", ?err, "{}: failed to establish a TIER1 connection",self.config.node_id());
                }
            }
            tracing::debug!(target:"network","{}: establishing new connections DONE",self.config.node_id());
        }
    }

    pub fn get_tier1_peer(
        &self,
        account_id: &AccountId,
    ) -> Option<(PeerId, Arc<connection::Connection>)> {
        let tier1 = self.tier1.load();
        let accounts_data = self.accounts_data.load();
        for data in accounts_data.by_account.get(account_id)?.values() {
            let peer_id = match &data.peer_id {
                Some(id) => id,
                None => continue,
            };
            tracing::debug!(target:"test", ?account_id, ?peer_id, "TIER1 peer lookup");

            tracing::debug!(target:"test", "TIER1 connections: {:?}", tier1.ready.keys().collect::<Vec<_>>());
            if let Some(conn) = tier1.ready.get(peer_id) {
                tracing::debug!(target:"test", ?peer_id, "got the connection!");
                return Some((peer_id.clone(), conn.clone()));
            }
        }
        return None;
    }

    // Finds a TIER1 connection for the given AccountId.
    // It is expected to perform <10 lookups total on average,
    // so the call latency should be negligible wrt sending a TCP packet.
    // If not, consider precomputing the AccountId -> Connection mapping.
    pub fn get_tier1_proxy(
        &self,
        account_id: &AccountId,
    ) -> Option<(PeerId, Arc<connection::Connection>)> {
        // Prefer direct connections.
        if let Some(res) = self.get_tier1_peer(account_id) {
            return Some(res);
        }
        // In case there is no direct connection and our node is a TIER1 validator, use a proxy.
        // TODO(gprusak): add a check that our node is actually a TIER1 validator.
        let tier1 = self.tier1.load();
        let accounts_data = self.accounts_data.load();
        for data in accounts_data.by_account.get(account_id)?.values() {
            let peer_id = match &data.peer_id {
                Some(id) => id,
                None => continue,
            };
            for proxy in &data.peers {
                if let Some(conn) = tier1.ready.get(&proxy.peer_id) {
                    return Some((peer_id.clone(), conn.clone()));
                }
            }
        }
        None
    }
}