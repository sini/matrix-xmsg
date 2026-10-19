use async_trait::async_trait;
use matrix_sdk::{
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    encryption::{BackupDownloadStrategy, EncryptionSettings},
    ruma::{
        events::{
            relation::{InReplyTo, Replacement, Reply as RelationReply, Thread},
            room::{
                encrypted::{
                    EncryptedEventScheme, MegolmV1AesSha2ContentInit,
                    Relation as EncryptedRelation, RoomEncryptedEventContent,
                },
                message::{Relation, RoomMessageEventContent},
            },
        },
        DeviceId, OwnedEventId, RoomId, UserId,
    },
    Client, SessionMeta, SessionTokens,
};
use matrix_xmsg::bot::{
    handle_inbox_delivery_with_guard, register_event_handlers_with_startup_ts, BotOutcome,
};
use matrix_xmsg::config::{Admission, Config, GuardFailureMode};
use matrix_xmsg::context::{format_context_line_with_screening, UndecryptablePlaceholderLine};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::{is_crypto_store_empty, MatrixClient, MatrixSdkClient};
use matrix_xmsg::store::{BotReplyEventRecord, ForwardedMessageRecord, Store};
use matrix_xmsg::xmsg::{
    GuardClient, GuardEnvelope, GuardVerdict, SendResponse, SvcDelivery, SvcInbox, XmsgClient,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const BOT_MXID: &str = "@genie:example.org";
const ALICE_MXID: &str = "@alice:example.org";
const OWNER_MXID: &str = "@owner:example.org";
const ENC_ROOM: &str = "!enc:example.org";
const PLAIN_ROOM: &str = "!plain:example.org";

fn percent_decode(input: &str) -> String {
    let mut out = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex_str) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(val) = u8::from_str_radix(hex_str, 16) {
                    out.push(val);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[derive(Default)]
struct RoomData {
    encrypted: bool,
    members: Vec<String>,
    timeline: Vec<Value>,
    sync_cursors: HashMap<(String, String), usize>,
}

#[derive(Default)]
struct HomeserverState {
    tokens: HashMap<String, (String, String)>,
    device_keys: HashMap<String, HashMap<String, Value>>,
    one_time_keys: HashMap<String, HashMap<String, BTreeMap<String, Value>>>,
    master_keys: HashMap<String, Value>,
    self_signing_keys: HashMap<String, Value>,
    user_signing_keys: HashMap<String, Value>,
    to_device_inbox: HashMap<(String, String), Vec<Value>>,
    held_to_device: Vec<((String, String), Value)>,
    hold_to_device_for_user: Option<String>,
    rooms: HashMap<String, RoomData>,
    sent_events: Vec<(String, String, Value)>,
    redactions: Vec<(String, String, Option<String>)>,
    account_data: HashMap<(String, String), Value>,
    backup_version: Option<Value>,
    backup_keys: HashMap<(String, String), Value>,
    keys_upload_count: usize,
    batch_counter: usize,
    event_counter: usize,
}

#[derive(Clone, Default)]
struct StatefulMatrixHomeserver {
    state: Arc<Mutex<HomeserverState>>,
}

impl StatefulMatrixHomeserver {
    fn new() -> Self {
        let hs = Self::default();
        {
            let mut st = hs.state.lock().unwrap();
            st.tokens.insert(
                "tok_bot_1".to_string(),
                (BOT_MXID.to_string(), "MATRIX_XMSG".to_string()),
            );
            st.tokens.insert(
                "tok_bot_2".to_string(),
                (BOT_MXID.to_string(), "MATRIX_XMSG_2".to_string()),
            );
            st.tokens.insert(
                "tok_alice".to_string(),
                (ALICE_MXID.to_string(), "ALICE_DEV".to_string()),
            );
            let members = vec![
                BOT_MXID.to_string(),
                ALICE_MXID.to_string(),
                OWNER_MXID.to_string(),
            ];
            st.rooms.insert(
                ENC_ROOM.to_string(),
                RoomData {
                    encrypted: true,
                    members: members.clone(),
                    timeline: Vec::new(),
                    sync_cursors: HashMap::new(),
                },
            );
            st.rooms.insert(
                PLAIN_ROOM.to_string(),
                RoomData {
                    encrypted: false,
                    members,
                    timeline: Vec::new(),
                    sync_cursors: HashMap::new(),
                },
            );
        }
        hs
    }

    fn set_hold_to_device_for(&self, user_id: Option<&str>) {
        self.state.lock().unwrap().hold_to_device_for_user = user_id.map(str::to_string);
    }

    fn release_held_to_device(&self) {
        let mut st = self.state.lock().unwrap();
        st.hold_to_device_for_user = None;
        let held: Vec<_> = st.held_to_device.drain(..).collect();
        for (target, ev) in held {
            st.to_device_inbox.entry(target).or_default().push(ev);
        }
    }

    fn inject_timeline_event(&self, room_id: &str, ev: Value) {
        let mut st = self.state.lock().unwrap();
        if let Some(room) = st.rooms.get_mut(room_id) {
            room.timeline.push(ev);
        }
    }

    fn sent_events_in_room(&self, room_id: &str) -> Vec<(String, Value)> {
        let st = self.state.lock().unwrap();
        st.sent_events
            .iter()
            .filter(|(r, _, _)| r == room_id)
            .map(|(_, t, v)| (t.clone(), v.clone()))
            .collect()
    }
}

impl Respond for StatefulMatrixHomeserver {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let path = request.url.path();
        let method = request.method.as_str();

        if method == "GET" && path == "/_matrix/client/versions" {
            return ResponseTemplate::new(200).set_body_json(json!({
                "versions": ["v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6", "v1.7", "v1.8", "v1.9", "v1.10", "v1.11"]
            }));
        }

        let auth_header = request
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let token = auth_header.strip_prefix("Bearer ").unwrap_or("");

        let mut st = self.state.lock().unwrap();
        let Some((caller_user, caller_device)) = st.tokens.get(token).cloned() else {
            return ResponseTemplate::new(401).set_body_json(json!({
                "errcode": "M_UNKNOWN_TOKEN",
                "error": "Invalid or missing access token"
            }));
        };

        if method == "GET" && path == "/_matrix/client/v3/account/whoami" {
            if caller_device.is_empty() {
                return ResponseTemplate::new(200).set_body_json(json!({
                    "user_id": caller_user
                }));
            }
            return ResponseTemplate::new(200).set_body_json(json!({
                "user_id": caller_user,
                "device_id": caller_device
            }));
        }

        if method == "PUT" && path.starts_with("/_matrix/client/v3/devices/") {
            return ResponseTemplate::new(200).set_body_json(json!({}));
        }

        if method == "POST" && path == "/_matrix/client/v3/keys/upload" {
            st.keys_upload_count += 1;
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
            if let Some(dk) = body.get("device_keys") {
                st.device_keys
                    .entry(caller_user.clone())
                    .or_default()
                    .insert(caller_device.clone(), dk.clone());
            }
            if let Some(otks) = body.get("one_time_keys").and_then(|v| v.as_object()) {
                let entry = st
                    .one_time_keys
                    .entry(caller_user.clone())
                    .or_default()
                    .entry(caller_device.clone())
                    .or_default();
                for (k, v) in otks {
                    entry.insert(k.clone(), v.clone());
                }
            }
            let count = st
                .one_time_keys
                .get(&caller_user)
                .and_then(|d| d.get(&caller_device))
                .map(|m| m.len())
                .unwrap_or(0)
                .max(50);
            return ResponseTemplate::new(200).set_body_json(json!({
                "one_time_key_counts": {
                    "signed_curve25519": count
                }
            }));
        }

        if method == "POST" && path == "/_matrix/client/v3/keys/device_signing/upload" {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
            if let Some(mk) = body.get("master_key") {
                st.master_keys.insert(caller_user.clone(), mk.clone());
            }
            if let Some(ssk) = body.get("self_signing_key") {
                st.self_signing_keys
                    .insert(caller_user.clone(), ssk.clone());
            }
            if let Some(usk) = body.get("user_signing_key") {
                st.user_signing_keys
                    .insert(caller_user.clone(), usk.clone());
            }
            return ResponseTemplate::new(200).set_body_json(json!({}));
        }

        if method == "POST" && path == "/_matrix/client/v3/keys/signatures/upload" {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
            if let Some(users_obj) = body.as_object() {
                for (u_id, targets) in users_obj {
                    if let Some(targets_obj) = targets.as_object() {
                        for (t_id, signed_val) in targets_obj {
                            if let Some(new_sigs) =
                                signed_val.get("signatures").and_then(|s| s.as_object())
                            {
                                if let Some(dev_val) =
                                    st.device_keys.get_mut(u_id).and_then(|m| m.get_mut(t_id))
                                {
                                    if let Some(existing_sigs) = dev_val
                                        .get_mut("signatures")
                                        .and_then(|s| s.as_object_mut())
                                    {
                                        for (sig_user, sig_map) in new_sigs {
                                            if let Some(sig_map_obj) = sig_map.as_object() {
                                                let user_entry = existing_sigs
                                                    .entry(sig_user.clone())
                                                    .or_insert_with(|| json!({}));
                                                if let Some(ue) = user_entry.as_object_mut() {
                                                    for (k, v) in sig_map_obj {
                                                        ue.insert(k.clone(), v.clone());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else if let Some(mk_val) = st.master_keys.get_mut(u_id) {
                                    if let Some(existing_sigs) =
                                        mk_val.get_mut("signatures").and_then(|s| s.as_object_mut())
                                    {
                                        for (sig_user, sig_map) in new_sigs {
                                            if let Some(sig_map_obj) = sig_map.as_object() {
                                                let user_entry = existing_sigs
                                                    .entry(sig_user.clone())
                                                    .or_insert_with(|| json!({}));
                                                if let Some(ue) = user_entry.as_object_mut() {
                                                    for (k, v) in sig_map_obj {
                                                        ue.insert(k.clone(), v.clone());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return ResponseTemplate::new(200).set_body_json(json!({ "failures": {} }));
        }

        if method == "POST" && path == "/_matrix/client/v3/keys/query" {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
            let mut out_dk = serde_json::Map::new();
            let mut out_mk = serde_json::Map::new();
            let mut out_ssk = serde_json::Map::new();
            let mut out_usk = serde_json::Map::new();

            if let Some(req_users) = body.get("device_keys").and_then(|v| v.as_object()) {
                for u_id in req_users.keys() {
                    let devs = st
                        .device_keys
                        .get(u_id)
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .collect::<serde_json::Map<String, Value>>();
                    out_dk.insert(u_id.clone(), Value::Object(devs));
                    if let Some(mk) = st.master_keys.get(u_id) {
                        out_mk.insert(u_id.clone(), mk.clone());
                    }
                    if let Some(ssk) = st.self_signing_keys.get(u_id) {
                        out_ssk.insert(u_id.clone(), ssk.clone());
                    }
                    if let Some(usk) = st.user_signing_keys.get(u_id) {
                        out_usk.insert(u_id.clone(), usk.clone());
                    }
                }
            }
            return ResponseTemplate::new(200).set_body_json(json!({
                "device_keys": out_dk,
                "master_keys": out_mk,
                "self_signing_keys": out_ssk,
                "user_signing_keys": out_usk,
                "failures": {}
            }));
        }

        if method == "POST" && path == "/_matrix/client/v3/keys/claim" {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
            let mut claimed = serde_json::Map::new();
            if let Some(req_users) = body.get("one_time_keys").and_then(|v| v.as_object()) {
                for (u_id, dev_map) in req_users {
                    let mut user_claimed = serde_json::Map::new();
                    if let Some(devs) = dev_map.as_object() {
                        for d_id in devs.keys() {
                            if let Some(otks) =
                                st.one_time_keys.get_mut(u_id).and_then(|m| m.get_mut(d_id))
                            {
                                if let Some(first_key) = otks.keys().next().cloned() {
                                    if let Some(val) = otks.remove(&first_key) {
                                        let mut single = serde_json::Map::new();
                                        single.insert(first_key, val);
                                        user_claimed.insert(d_id.clone(), Value::Object(single));
                                    }
                                }
                            }
                        }
                    }
                    claimed.insert(u_id.clone(), Value::Object(user_claimed));
                }
            }
            return ResponseTemplate::new(200).set_body_json(json!({
                "one_time_keys": claimed,
                "failures": {}
            }));
        }

        if method == "PUT" && path.starts_with("/_matrix/client/v3/sendToDevice/") {
            let rest = &path["/_matrix/client/v3/sendToDevice/".len()..];
            let ev_type = percent_decode(rest.split('/').next().unwrap_or(""));
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
            let hold_user = st.hold_to_device_for_user.clone();
            if let Some(msgs) = body.get("messages").and_then(|v| v.as_object()) {
                for (u_id, dev_map) in msgs {
                    if let Some(devs) = dev_map.as_object() {
                        for (d_id, content) in devs {
                            let to_dev_ev = json!({
                                "type": ev_type,
                                "sender": caller_user,
                                "content": content
                            });
                            if hold_user.as_deref() == Some(u_id.as_str()) {
                                st.held_to_device
                                    .push(((u_id.clone(), d_id.clone()), to_dev_ev));
                            } else {
                                st.to_device_inbox
                                    .entry((u_id.clone(), d_id.clone()))
                                    .or_default()
                                    .push(to_dev_ev);
                            }
                        }
                    }
                }
            }
            return ResponseTemplate::new(200).set_body_json(json!({}));
        }

        if method == "GET" && path == "/_matrix/client/v3/sync" {
            st.batch_counter += 1;
            let batch_num = st.batch_counter;
            let to_device = st
                .to_device_inbox
                .remove(&(caller_user.clone(), caller_device.clone()))
                .unwrap_or_default();

            let room_ids: Vec<String> = st.rooms.keys().cloned().collect();
            let mut join_rooms = serde_json::Map::new();
            for r_id in room_ids {
                let room = st.rooms.get_mut(&r_id).unwrap();
                let key = (caller_user.clone(), caller_device.clone());
                let prev_cursor = room.sync_cursors.get(&key).copied();
                let start_idx = prev_cursor.unwrap_or(0);
                let new_events: Vec<Value> = room.timeline[start_idx..].to_vec();
                room.sync_cursors.insert(key, room.timeline.len());

                let mut state_events = Vec::new();
                if prev_cursor.is_none() {
                    for (idx, m) in room.members.iter().enumerate() {
                        state_events.push(json!({
                            "type": "m.room.member",
                            "state_key": m,
                            "sender": m,
                            "event_id": format!("$mem_{idx}_{}", r_id.replace(['!', ':'], "_")),
                            "origin_server_ts": 1000,
                            "content": {
                                "membership": "join",
                                "displayname": m
                            }
                        }));
                    }
                    if room.encrypted {
                        state_events.push(json!({
                            "type": "m.room.encryption",
                            "state_key": "",
                            "sender": ALICE_MXID,
                            "event_id": format!("$enc_state_{}", r_id.replace(['!', ':'], "_")),
                            "origin_server_ts": 1000,
                            "content": {
                                "algorithm": "m.megolm.v1.aes-sha2"
                            }
                        }));
                    }
                }

                if prev_cursor.is_none() || !new_events.is_empty() {
                    join_rooms.insert(
                        r_id,
                        json!({
                            "state": { "events": state_events },
                            "timeline": {
                                "events": new_events,
                                "prev_batch": format!("p_{start_idx}")
                            }
                        }),
                    );
                }
            }

            let account_data_events: Vec<Value> = st
                .account_data
                .iter()
                .filter(|((u, _), _)| u == &caller_user)
                .map(|((_, ev_type), content)| {
                    json!({
                        "type": ev_type,
                        "content": content
                    })
                })
                .collect();

            let changed_users: Vec<String> = st.device_keys.keys().cloned().collect();
            return ResponseTemplate::new(200).set_body_json(json!({
                "next_batch": format!("s_{batch_num}"),
                "to_device": { "events": to_device },
                "account_data": { "events": account_data_events },
                "device_lists": { "changed": changed_users, "left": [] },
                "device_one_time_keys_count": { "signed_curve25519": 50 },
                "rooms": { "join": join_rooms }
            }));
        }

        if let Some((_, acct_rest)) = path.split_once("/account_data/") {
            let ev_type = percent_decode(acct_rest);
            if method == "PUT" {
                let body: Value =
                    serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
                st.account_data.insert((caller_user.clone(), ev_type), body);
                return ResponseTemplate::new(200).set_body_json(json!({}));
            }
            if method == "GET" {
                if let Some(val) = st.account_data.get(&(caller_user.clone(), ev_type)) {
                    return ResponseTemplate::new(200).set_body_json(val.clone());
                }
                return ResponseTemplate::new(404).set_body_json(json!({
                    "errcode": "M_NOT_FOUND",
                    "error": "Account data not found"
                }));
            }
        }

        if let Some((_, rk_rest)) = path.split_once("/room_keys/") {
            if rk_rest == "version" || rk_rest.starts_with("version/") {
                if method == "POST" {
                    let body: Value =
                        serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
                    st.backup_version = Some(json!({
                        "algorithm": body.get("algorithm").cloned().unwrap_or(json!("m.megolm_backup.v1.curve25519-aes-sha2")),
                        "auth_data": body.get("auth_data").cloned().unwrap_or_else(|| json!({})),
                        "count": st.backup_keys.len(),
                        "etag": "etag_1",
                        "version": "1"
                    }));
                    return ResponseTemplate::new(200).set_body_json(json!({ "version": "1" }));
                }
                if method == "GET" {
                    if let Some(mut v) = st.backup_version.clone() {
                        if let Some(obj) = v.as_object_mut() {
                            obj.insert("count".to_string(), json!(st.backup_keys.len()));
                        }
                        return ResponseTemplate::new(200).set_body_json(v);
                    }
                    return ResponseTemplate::new(404).set_body_json(json!({
                        "errcode": "M_NOT_FOUND",
                        "error": "No backup version found"
                    }));
                }
            }
            if rk_rest == "keys" {
                if method == "PUT" {
                    let body: Value =
                        serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
                    if let Some(rooms) = body.get("rooms").and_then(|v| v.as_object()) {
                        for (r_id, r_obj) in rooms {
                            if let Some(sessions) =
                                r_obj.get("sessions").and_then(|v| v.as_object())
                            {
                                for (s_id, s_data) in sessions {
                                    st.backup_keys
                                        .insert((r_id.clone(), s_id.clone()), s_data.clone());
                                }
                            }
                        }
                    }
                    let count = st.backup_keys.len();
                    return ResponseTemplate::new(200).set_body_json(json!({
                        "count": count,
                        "etag": format!("etag_{count}")
                    }));
                }
                if method == "GET" {
                    let mut rooms = serde_json::Map::new();
                    for ((r_id, s_id), s_data) in &st.backup_keys {
                        let r_entry = rooms
                            .entry(r_id.clone())
                            .or_insert_with(|| json!({ "sessions": {} }));
                        if let Some(sess_map) =
                            r_entry.get_mut("sessions").and_then(|v| v.as_object_mut())
                        {
                            sess_map.insert(s_id.clone(), s_data.clone());
                        }
                    }
                    return ResponseTemplate::new(200).set_body_json(json!({ "rooms": rooms }));
                }
            }
            if let Some(keys_sub) = rk_rest.strip_prefix("keys/") {
                let mut parts = keys_sub.splitn(2, '/');
                let r_id = percent_decode(parts.next().unwrap_or(""));
                let s_id = percent_decode(parts.next().unwrap_or(""));
                if method == "PUT" && !s_id.is_empty() {
                    let body: Value =
                        serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
                    st.backup_keys.insert((r_id, s_id), body);
                    let count = st.backup_keys.len();
                    return ResponseTemplate::new(200).set_body_json(json!({
                        "count": count,
                        "etag": format!("etag_{count}")
                    }));
                }
                if method == "GET" && !s_id.is_empty() {
                    if let Some(s_data) = st.backup_keys.get(&(r_id, s_id)) {
                        return ResponseTemplate::new(200).set_body_json(s_data.clone());
                    }
                    return ResponseTemplate::new(404).set_body_json(json!({
                        "errcode": "M_NOT_FOUND",
                        "error": "Backup key not found"
                    }));
                }
            }
        }

        let rooms_rest = path
            .strip_prefix("/_matrix/client/v3/rooms/")
            .or_else(|| path.strip_prefix("/_matrix/client/v1/rooms/"));
        if let Some(rest) = rooms_rest {
            let mut parts = rest.splitn(2, '/');
            let room_id = percent_decode(parts.next().unwrap_or(""));
            let subpath = parts.next().unwrap_or("");

            if method == "POST" && subpath == "join" {
                return ResponseTemplate::new(200).set_body_json(json!({ "room_id": room_id }));
            }

            if method == "GET" && subpath.starts_with("state/m.room.encryption") {
                let is_enc = st.rooms.get(&room_id).is_some_and(|r| r.encrypted);
                if is_enc {
                    return ResponseTemplate::new(200).set_body_json(json!({
                        "algorithm": "m.megolm.v1.aes-sha2"
                    }));
                }
                return ResponseTemplate::new(404).set_body_json(json!({
                    "errcode": "M_NOT_FOUND",
                    "error": "Event not found."
                }));
            }

            if method == "GET" && subpath == "joined_members" {
                let mut joined = serde_json::Map::new();
                if let Some(room) = st.rooms.get(&room_id) {
                    for m in &room.members {
                        joined.insert(m.clone(), json!({}));
                    }
                }
                return ResponseTemplate::new(200).set_body_json(json!({ "joined": joined }));
            }

            if method == "GET" && subpath == "members" {
                let mut chunk = Vec::new();
                if let Some(room) = st.rooms.get(&room_id) {
                    for (idx, m) in room.members.iter().enumerate() {
                        chunk.push(json!({
                            "type": "m.room.member",
                            "room_id": room_id,
                            "state_key": m,
                            "sender": m,
                            "event_id": format!("$mem_{idx}"),
                            "origin_server_ts": 1000,
                            "content": { "membership": "join" }
                        }));
                    }
                }
                return ResponseTemplate::new(200).set_body_json(json!({ "chunk": chunk }));
            }

            if method == "PUT" && subpath.starts_with("send/") {
                let send_rest = &subpath["send/".len()..];
                let ev_type = percent_decode(send_rest.split('/').next().unwrap_or(""));
                let content: Value =
                    serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
                st.event_counter += 1;
                let ev_id = format!("$ev_{}", st.event_counter);
                let now_ms = chrono::Utc::now().timestamp_millis().max(1_000_000);
                let full_ev = json!({
                    "type": ev_type,
                    "room_id": room_id,
                    "event_id": ev_id,
                    "sender": caller_user,
                    "origin_server_ts": now_ms,
                    "content": content
                });
                st.sent_events
                    .push((room_id.clone(), ev_type, content.clone()));
                if let Some(room) = st.rooms.get_mut(&room_id) {
                    room.timeline.push(full_ev);
                }
                return ResponseTemplate::new(200).set_body_json(json!({ "event_id": ev_id }));
            }

            if method == "PUT" && subpath.starts_with("redact/") {
                let red_rest = &subpath["redact/".len()..];
                let target_ev_id = percent_decode(red_rest.split('/').next().unwrap_or(""));
                let body: Value =
                    serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}));
                let reason = body
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                st.redactions.push((room_id.clone(), target_ev_id, reason));
                st.event_counter += 1;
                let ev_id = format!("$redact_{}", st.event_counter);
                return ResponseTemplate::new(200).set_body_json(json!({ "event_id": ev_id }));
            }

            if method == "GET" && subpath == "messages" {
                let chunk: Vec<Value> = st
                    .rooms
                    .get(&room_id)
                    .map(|r| r.timeline.iter().rev().cloned().collect())
                    .unwrap_or_default();
                return ResponseTemplate::new(200).set_body_json(json!({
                    "chunk": chunk,
                    "start": "t_end",
                    "end": "t_start"
                }));
            }

            if method == "GET" && subpath.starts_with("event/") {
                let target_id = percent_decode(&subpath["event/".len()..]);
                if let Some(room) = st.rooms.get(&room_id) {
                    if let Some(ev) = room
                        .timeline
                        .iter()
                        .find(|e| e.get("event_id").and_then(|v| v.as_str()) == Some(&target_id))
                    {
                        return ResponseTemplate::new(200).set_body_json(ev.clone());
                    }
                }
                return ResponseTemplate::new(404).set_body_json(json!({
                    "errcode": "M_NOT_FOUND",
                    "error": "Event not found"
                }));
            }

            if method == "GET" && subpath.starts_with("relations/") {
                let rel_rest = &subpath["relations/".len()..];
                let root_id = percent_decode(rel_rest.split('/').next().unwrap_or(""));
                let mut chunk = Vec::new();
                if let Some(room) = st.rooms.get(&room_id) {
                    for ev in &room.timeline {
                        let is_thread_rel = ev
                            .get("content")
                            .and_then(|c| c.get("m.relates_to"))
                            .is_some_and(|rel| {
                                rel.get("rel_type").and_then(|v| v.as_str()) == Some("m.thread")
                                    && rel.get("event_id").and_then(|v| v.as_str())
                                        == Some(root_id.as_str())
                            });
                        if is_thread_rel {
                            chunk.push(ev.clone());
                        }
                    }
                }
                return ResponseTemplate::new(200).set_body_json(json!({ "chunk": chunk }));
            }
        }

        ResponseTemplate::new(200).set_body_json(json!({}))
    }
}

#[derive(Clone, Default)]
struct RecordingXmsg {
    sent: Arc<Mutex<Vec<(String, String, String)>>>,
    replies: Arc<Mutex<Vec<(String, String)>>>,
    sessions: Arc<Mutex<VecDeque<Option<String>>>>,
    counter: Arc<AtomicUsize>,
}

impl RecordingXmsg {
    fn queue_session(&self, sess: Option<&str>) {
        self.sessions
            .lock()
            .unwrap()
            .push_back(sess.map(str::to_string));
    }
}

#[async_trait]
impl XmsgClient for RecordingXmsg {
    async fn send_message(
        &self,
        to: &str,
        sender: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        let id_num = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let id = format!("01M36MSG{id_num:06}");
        self.sent
            .lock()
            .unwrap()
            .push((to.to_string(), sender.to_string(), text.to_string()));
        let session_id = self
            .sessions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Some(format!("{to}@sini.bitstream")));
        Ok(SendResponse {
            message_id: id,
            session_id,
        })
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        let id_num = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let id = format!("01M36REP{id_num:06}");
        self.replies
            .lock()
            .unwrap()
            .push((message_id.to_string(), text.to_string()));
        let session_id = self
            .sessions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Some("reply@sini.bitstream".to_string()));
        Ok(SendResponse {
            message_id: id,
            session_id,
        })
    }
}

struct MockInbox {
    own_node: Option<String>,
    acked: Vec<String>,
}

impl MockInbox {
    fn new(node: &str) -> Self {
        Self {
            own_node: Some(node.to_string()),
            acked: Vec::new(),
        }
    }
}

#[async_trait]
impl SvcInbox for MockInbox {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        Ok(None)
    }

    async fn ack(&mut self, id: &str) -> Result<(), AppError> {
        self.acked.push(id.to_string());
        Ok(())
    }

    fn node(&self) -> Option<String> {
        self.own_node.clone()
    }
}

#[derive(Default)]
struct RecordingGuard {
    calls: Mutex<Vec<(String, GuardEnvelope)>>,
}

#[async_trait]
impl GuardClient for RecordingGuard {
    async fn check_guard(
        &self,
        guard_ref: &str,
        envelope: &GuardEnvelope,
    ) -> Result<GuardVerdict, AppError> {
        self.calls
            .lock()
            .unwrap()
            .push((guard_ref.to_string(), envelope.clone()));
        Ok(GuardVerdict {
            verdict: "allow".to_string(),
            reason: "clean".to_string(),
            cleaned_text: String::new(),
        })
    }
}

fn make_config(homeserver_url: &str, state_dir: &std::path::Path) -> Config {
    Config {
        homeserver_url: homeserver_url.to_string(),
        bot_mxid: BOT_MXID.to_string(),
        access_token_file: state_dir.join("token"),
        rooms: vec![ENC_ROOM.to_string(), PLAIN_ROOM.to_string()],
        trusted_mxids: vec![ALICE_MXID.to_string()],
        owner_mxid: OWNER_MXID.to_string(),
        admission: Admission::Public,
        xmsg_socket: state_dir.join("xmsg"),
        expert_ref: "claude".to_string(),
        expert_refs: None,
        room_routes: BTreeMap::new(),
        open_thread_nodes: vec![],
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
        screen_mentions: false,
        screen_solicited: false,
        guard_failure_mode: GuardFailureMode::Closed,
        guard_outage_dm_window_secs: 3600,
        inbox_retry_budget: 10,
        store_passphrase_file: Some(state_dir.join("store-passphrase")),
        undecryptable_wait_secs: 1,
        history_n: 30,
        history_byte_cap: 12288,
        resync_byte_cap: 65536,
        resync_page_bytes: 8192,
        context_max_lines: 20,
        context_max_bytes: 8192,
        sender_window_secs: 900,
        context_rate_limit_count: 20,
        context_rate_limit_window_secs: 600,
        rate_limit_count: 100,
        rate_limit_window_secs: 600,
        size_cap_bytes: 4096,
        answer_timeout_secs: 600,
        session_live_secs: 3600,
        db_path: state_dir.join("matrix-xmsg.db"),
    }
}

async fn build_alice_client(homeserver_url: &str, dir: &std::path::Path) -> Client {
    std::fs::create_dir_all(dir).unwrap();
    let client = Client::builder()
        .homeserver_url(homeserver_url)
        .sqlite_store(dir, Some("alice-passphrase"))
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: false,
            backup_download_strategy: BackupDownloadStrategy::AfterDecryptionFailure,
            auto_enable_backups: false,
        })
        .with_enable_share_history_on_invite(true)
        .build()
        .await
        .unwrap();
    let session = MatrixSession {
        meta: SessionMeta {
            user_id: <&UserId>::try_from(ALICE_MXID).unwrap().to_owned(),
            device_id: <&DeviceId>::from("ALICE_DEV").to_owned(),
        },
        tokens: SessionTokens {
            access_token: "tok_alice".to_string(),
            refresh_token: None,
        },
    };
    client.restore_session(session).await.unwrap();
    client.sync_once(SyncSettings::default()).await.unwrap();
    client
        .encryption()
        .bootstrap_cross_signing(None)
        .await
        .unwrap();
    client.sync_once(SyncSettings::default()).await.unwrap();
    client
}

async fn sync_until_sent_count(bot: &MatrixSdkClient, xmsg: &RecordingXmsg, expected: usize) {
    for _ in 0..10 {
        bot.inner()
            .sync_once(SyncSettings::default().timeout(Duration::ZERO))
            .await
            .unwrap();
        for _ in 0..20 {
            if xmsg.sent.lock().unwrap().len() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }
    panic!(
        "Timed out waiting for xmsg.sent.len() >= {expected}, got {}",
        xmsg.sent.lock().unwrap().len()
    );
}

// =================================================================================================
// §7.2: Every forward-path oracle runs in both room kinds (encrypted and unencrypted):
// mention, reply, edit, thread bootstrap, thread delta, M16 rebootstrap, resync,
// context (room/event/thread), edit-by-request, and redact-by-request.
// =================================================================================================
async fn exercise_room_forward_paths(
    room_id_str: &str,
    expect_encrypted_wire: bool,
) -> Vec<String> {
    let server = MockServer::start().await;
    let hs = StatefulMatrixHomeserver::new();
    Mock::given(wiremock::matchers::any())
        .respond_with(hs.clone())
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let bot_dir = tmp.path().join("bot");
    let alice_dir = tmp.path().join("alice");

    let alice = build_alice_client(&server.uri(), &alice_dir).await;
    let bot = Arc::new(
        MatrixSdkClient::new_with_e2ee(
            &server.uri(),
            BOT_MXID,
            "tok_bot_1",
            &bot_dir,
            "bot-store-passphrase",
            false,
            None,
        )
        .await
        .unwrap(),
    );

    let config = Arc::new(make_config(&server.uri(), &bot_dir));
    let store = Arc::new(Store::new_in_memory().unwrap());
    let xmsg = Arc::new(RecordingXmsg::default());
    let guard = RecordingGuard::default();

    let _tracker = register_event_handlers_with_startup_ts(
        bot.inner(),
        config.clone(),
        bot.clone(),
        xmsg.clone(),
        store.clone(),
        0,
    );

    // Sync both so both have room state and device keys
    bot.inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    alice.sync_once(SyncSettings::default()).await.unwrap();
    bot.resolve_rooms(&[room_id_str.to_string()]).await.unwrap();

    let r_id = <&RoomId>::try_from(room_id_str).unwrap();
    let alice_room = alice.get_room(r_id).unwrap();
    let mut transcripts = Vec::new();

    // 1. Plain mention
    let mention_resp = alice_room
        .send(RoomMessageEventContent::text_plain(format!(
            "{BOT_MXID} hello from room"
        )))
        .await
        .unwrap();
    let mention_ev_id = mention_resp.response.event_id.to_string();
    sync_until_sent_count(&bot, &xmsg, 1).await;
    transcripts.push(xmsg.sent.lock().unwrap().last().unwrap().2.clone());

    // Expert replies to mention -> bot posts encrypted/unencrypted reply in room
    let mention_fwd_id = "01M36MSG000001".to_string();
    let mut inbox = MockInbox::new("sini.bitstream");
    let reply_del = SvcDelivery {
        message_id: "01M36DEL_REPLY".to_string(),
        from_name: "claude@sini.bitstream".to_string(),
        text: "bot answer to mention".to_string(),
        envelope: String::new(),
        in_reply_to: Some(mention_fwd_id.clone()),
        origin: Some(json!({ "service": "claude", "node": "sini.bitstream" })),
    };
    let out_reply = handle_inbox_delivery_with_guard(
        &reply_del,
        &config,
        bot.as_ref(),
        &store,
        xmsg.as_ref(),
        &guard,
        &mut inbox,
        1_000,
    )
    .await
    .unwrap();
    assert_eq!(out_reply, BotOutcome::Replied);

    // Find the bot's reply event ID from the timeline
    let bot_reply_ev_id = {
        let st = hs.state.lock().unwrap();
        let room = st.rooms.get(room_id_str).unwrap();
        room.timeline
            .last()
            .unwrap()
            .get("event_id")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    };

    // 2. Reply-to-bot (m.in_reply_to)
    let mut reply_content = RoomMessageEventContent::text_plain("follow-up reply to bot");
    reply_content.relates_to = Some(Relation::Reply(RelationReply::new(InReplyTo::new(
        <&matrix_sdk::ruma::EventId>::try_from(bot_reply_ev_id.as_str())
            .unwrap()
            .to_owned(),
    ))));
    alice_room.send(reply_content).await.unwrap();
    sync_until_sent_count(&bot, &xmsg, 2).await;
    transcripts.push(xmsg.sent.lock().unwrap().last().unwrap().2.clone());

    // 3. Edit-into-mention (m.replace)
    let orig_resp = alice_room
        .send(RoomMessageEventContent::text_plain("untagged message"))
        .await
        .unwrap();
    let orig_ev_id = orig_resp.response.event_id;
    bot.inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();

    let mut edit_content =
        RoomMessageEventContent::text_plain(format!("* {BOT_MXID} edited into mention"));
    edit_content.relates_to = Some(Relation::Replacement(Replacement::new(
        orig_ev_id,
        RoomMessageEventContent::text_plain(format!("{BOT_MXID} edited into mention")).into(),
    )));
    alice_room.send(edit_content).await.unwrap();
    sync_until_sent_count(&bot, &xmsg, 3).await;
    transcripts.push(xmsg.sent.lock().unwrap().last().unwrap().2.clone());

    // 4. Engaged thread: first follow-up (delta on existing session) + session-change rebootstrap
    let root_ev: OwnedEventId = mention_ev_id.as_str().try_into().unwrap();
    let mut thread_msg1 = RoomMessageEventContent::text_plain("thread follow-up 1");
    thread_msg1.relates_to = Some(Relation::Thread(Thread::plain(
        root_ev.clone(),
        root_ev.clone(),
    )));
    xmsg.queue_session(Some("claude@sini.bitstream"));
    alice_room.send(thread_msg1).await.unwrap();
    sync_until_sent_count(&bot, &xmsg, 4).await;
    transcripts.push(xmsg.sent.lock().unwrap().last().unwrap().2.clone());

    // Session change on next thread message -> triggers M16 rebootstrap
    let mut thread_msg2 = RoomMessageEventContent::text_plain("thread follow-up 2 session change");
    thread_msg2.relates_to = Some(Relation::Thread(Thread::plain(
        root_ev.clone(),
        root_ev.clone(),
    )));
    xmsg.queue_session(Some("new-claude@sini.bitstream"));
    xmsg.queue_session(Some("new-claude@sini.bitstream"));
    alice_room.send(thread_msg2).await.unwrap();
    sync_until_sent_count(&bot, &xmsg, 6).await;
    {
        let sent = xmsg.sent.lock().unwrap();
        transcripts.push(sent[sent.len() - 2].2.clone());
        transcripts.push(sent[sent.len() - 1].2.clone());
    }

    // 5. Resync and M33 context (room, event, thread)
    let latest_fwd_id = "01M36MSG000006".to_string();
    let resync_del = SvcDelivery {
        message_id: "01M36RESYNC".to_string(),
        from_name: "new-claude@sini.bitstream".to_string(),
        text: json!({ "resync": true }).to_string(),
        envelope: String::new(),
        in_reply_to: Some(latest_fwd_id.clone()),
        origin: Some(json!({ "service": "new-claude", "node": "sini.bitstream" })),
    };
    let out_resync = handle_inbox_delivery_with_guard(
        &resync_del,
        &config,
        bot.as_ref(),
        &store,
        xmsg.as_ref(),
        &guard,
        &mut inbox,
        1_010,
    )
    .await
    .unwrap();
    assert_eq!(out_resync, BotOutcome::Resynced);
    transcripts.push(xmsg.replies.lock().unwrap().last().unwrap().1.clone());

    for (id, ctx_req) in [
        (
            "01M36CTX_ROOM",
            json!({ "context": { "scope": "room", "limit": 5 } }),
        ),
        (
            "01M36CTX_EVENT",
            json!({ "context": { "scope": "event", "event": mention_ev_id } }),
        ),
        (
            "01M36CTX_THREAD",
            json!({ "context": { "scope": "thread", "thread": mention_ev_id } }),
        ),
    ] {
        let del = SvcDelivery {
            message_id: id.to_string(),
            from_name: "new-claude@sini.bitstream".to_string(),
            text: ctx_req.to_string(),
            envelope: String::new(),
            in_reply_to: Some(latest_fwd_id.clone()),
            origin: Some(json!({ "service": "new-claude", "node": "sini.bitstream" })),
        };
        let out_ctx = handle_inbox_delivery_with_guard(
            &del,
            &config,
            bot.as_ref(),
            &store,
            xmsg.as_ref(),
            &guard,
            &mut inbox,
            1_015,
        )
        .await
        .unwrap();
        assert_eq!(out_ctx, BotOutcome::ContextAnswered);
        transcripts.push(xmsg.replies.lock().unwrap().last().unwrap().1.clone());
    }

    // 6. Edit-by-request and Redact-by-request on bot's own message (keyed by reply delivery message_id)
    let edit_req_del = SvcDelivery {
        message_id: "01M36EDIT_REQ".to_string(),
        from_name: "claude@sini.bitstream".to_string(),
        text: json!({ "edit": "01M36DEL_REPLY", "answer": "edited bot answer" }).to_string(),
        envelope: String::new(),
        in_reply_to: Some(mention_fwd_id.clone()),
        origin: Some(json!({ "service": "claude", "node": "sini.bitstream" })),
    };
    let out_edit_req = handle_inbox_delivery_with_guard(
        &edit_req_del,
        &config,
        bot.as_ref(),
        &store,
        xmsg.as_ref(),
        &guard,
        &mut inbox,
        1_020,
    )
    .await
    .unwrap();
    assert_eq!(out_edit_req, BotOutcome::Edited);

    let redact_req_del = SvcDelivery {
        message_id: "01M36REDACT_REQ".to_string(),
        from_name: "claude@sini.bitstream".to_string(),
        text: json!({ "redact": "01M36DEL_REPLY", "reason": "cleanup" }).to_string(),
        envelope: String::new(),
        in_reply_to: Some(mention_fwd_id),
        origin: Some(json!({ "service": "claude", "node": "sini.bitstream" })),
    };
    let out_redact_req = handle_inbox_delivery_with_guard(
        &redact_req_del,
        &config,
        bot.as_ref(),
        &store,
        xmsg.as_ref(),
        &guard,
        &mut inbox,
        1_025,
    )
    .await
    .unwrap();
    assert_eq!(out_redact_req, BotOutcome::Redacted);

    // Verify wire encryption invariant on all non-reaction message events (per Matrix spec, m.reaction is unencrypted)
    let wire_events = hs.sent_events_in_room(room_id_str);
    let msg_events: Vec<_> = wire_events
        .iter()
        .filter(|(ev_type, _)| ev_type != "m.reaction")
        .collect();
    assert!(!msg_events.is_empty());
    for (ev_type, _) in msg_events {
        if expect_encrypted_wire {
            assert_eq!(
                ev_type, "m.room.encrypted",
                "Every message sent in encrypted room {room_id_str} MUST be m.room.encrypted on wire"
            );
        } else {
            assert_eq!(
                ev_type, "m.room.message",
                "Messages in unencrypted room {room_id_str} must be m.room.message"
            );
        }
    }

    transcripts
}

#[tokio::test]
async fn m36_oracle_all_forward_paths_encrypted_and_unencrypted() {
    let enc_transcripts = exercise_room_forward_paths(ENC_ROOM, true).await;
    let plain_transcripts = exercise_room_forward_paths(PLAIN_ROOM, false).await;

    assert_eq!(enc_transcripts.len(), plain_transcripts.len());
    assert!(enc_transcripts[0].contains("hello from room"));
    assert!(enc_transcripts[1].contains("follow-up reply to bot"));
    assert!(enc_transcripts[2].contains("edited into mention"));
    assert!(enc_transcripts[3].contains("thread follow-up 1"));
    assert!(enc_transcripts[5].contains("thread follow-up 2 session change"));
    assert!(enc_transcripts[6].contains("thread follow-up 1")); // resync
    assert!(enc_transcripts[7].contains("hello from room")); // room context
    assert!(enc_transcripts[8].contains("hello from room")); // event context
    assert!(enc_transcripts[9].contains("thread follow-up 1")); // thread context
}

// =================================================================================================
// §7.3 Cell: Late room key arriving in later sync => decrypted and forwarded within
// undecryptable_wait_secs, with the sync loop not blocked meanwhile (gate C5).
// =================================================================================================
#[tokio::test]
async fn m36_cell_late_room_key_spawned_off_sync_loop() {
    let server = MockServer::start().await;
    let hs = StatefulMatrixHomeserver::new();
    Mock::given(wiremock::matchers::any())
        .respond_with(hs.clone())
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let bot_dir = tmp.path().join("bot");
    let alice_dir = tmp.path().join("alice");

    let alice = build_alice_client(&server.uri(), &alice_dir).await;
    let bot = Arc::new(
        MatrixSdkClient::new_with_e2ee(
            &server.uri(),
            BOT_MXID,
            "tok_bot_1",
            &bot_dir,
            "bot-store-passphrase",
            false,
            None,
        )
        .await
        .unwrap(),
    );

    let mut cfg = make_config(&server.uri(), &bot_dir);
    cfg.undecryptable_wait_secs = 5;
    let config = Arc::new(cfg);
    let store = Arc::new(Store::new_in_memory().unwrap());
    let xmsg = Arc::new(RecordingXmsg::default());

    let _tracker = register_event_handlers_with_startup_ts(
        bot.inner(),
        config,
        bot.clone(),
        xmsg.clone(),
        store,
        0,
    );

    bot.inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    alice.sync_once(SyncSettings::default()).await.unwrap();
    bot.resolve_rooms(&[ENC_ROOM.to_string()]).await.unwrap();

    // Hold to-device messages for BOT_MXID so the encrypted event arrives BEFORE its room key
    hs.set_hold_to_device_for(Some(BOT_MXID));

    let alice_room = alice
        .get_room(<&RoomId>::try_from(ENC_ROOM).unwrap())
        .unwrap();
    alice_room
        .send(RoomMessageEventContent::text_plain(format!(
            "{BOT_MXID} message with late room key"
        )))
        .await
        .unwrap();

    // Sync 1: delivers m.room.encrypted WITHOUT the room key.
    // Because the late-key wait is spawned off the sync loop, sync_once returns immediately (< 500ms)!
    let t0 = std::time::Instant::now();
    bot.inner()
        .sync_once(SyncSettings::default().timeout(Duration::ZERO))
        .await
        .unwrap();
    assert!(
        t0.elapsed() < Duration::from_secs(1),
        "Sync loop must not be blocked by undecryptable_wait_secs=5s!"
    );
    assert_eq!(
        xmsg.sent.lock().unwrap().len(),
        0,
        "Cannot be forwarded before the room key arrives"
    );

    // Give the spawned handler task a brief moment to subscribe to room_keys_received_stream()
    tokio::time::sleep(Duration::from_millis(25)).await;

    // Release the held to-device room key and run Sync 2 so the bot receives the room key
    hs.release_held_to_device();
    bot.inner()
        .sync_once(SyncSettings::default().timeout(Duration::ZERO))
        .await
        .unwrap();

    // Wait for the spawned late-key task to wake on room_keys_received_stream(), decrypt, and forward
    for _ in 0..40 {
        if !xmsg.sent.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let sent = xmsg.sent.lock().unwrap();
    assert_eq!(
        sent.len(),
        1,
        "Late-key message must be decrypted and forwarded once key arrives"
    );
    assert!(sent[0].2.contains("message with late room key"));
}

// =================================================================================================
// §7.3 Cells:
// - Undecryptable reply to the bot => 🔒 reaction and no forward; elsewhere => silent (P5)
// - Placeholders in context/resync carry event_id, sender, ts, thread_root?, undecryptable: true
//   and are never sent to the guard (C7)
// - Own undecryptable message edit/redact checks unencrypted sender only (P7)
// =================================================================================================
#[tokio::test]
async fn m36_cells_undecryptable_reaction_placeholders_and_own_message_ops() {
    let server = MockServer::start().await;
    let hs = StatefulMatrixHomeserver::new();
    Mock::given(wiremock::matchers::any())
        .respond_with(hs.clone())
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let bot_dir = tmp.path().join("bot");
    let bot = Arc::new(
        MatrixSdkClient::new_with_e2ee(
            &server.uri(),
            BOT_MXID,
            "tok_bot_1",
            &bot_dir,
            "bot-store-passphrase",
            false,
            None,
        )
        .await
        .unwrap(),
    );

    let mut cfg = make_config(&server.uri(), &bot_dir);
    cfg.undecryptable_wait_secs = 0;
    cfg.screen_mentions = true;
    let config = Arc::new(cfg);
    let store = Arc::new(Store::new_in_memory().unwrap());
    let xmsg = Arc::new(RecordingXmsg::default());
    let guard = RecordingGuard::default();

    store
        .record_bot_message("$bot_post_1", "$bot_post_1", 1_000)
        .unwrap();

    let _tracker = register_event_handlers_with_startup_ts(
        bot.inner(),
        config.clone(),
        bot.clone(),
        xmsg.clone(),
        store.clone(),
        0,
    );

    bot.inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    bot.resolve_rooms(&[ENC_ROOM.to_string()]).await.unwrap();

    // 1. Inject an undecryptable top-level event (no relation to bot) -> should NOT react with 🔒
    let bogus_cipher = RoomEncryptedEventContent::new(
        EncryptedEventScheme::MegolmV1AesSha2(
            MegolmV1AesSha2ContentInit {
                ciphertext: "bogus_ciphertext_1".to_string(),
                sender_key: "bogus_sender_key".to_string(),
                device_id: "UNKNOWN_DEV".into(),
                session_id: "unknown_session_1".to_string(),
            }
            .into(),
        ),
        None,
    );
    hs.inject_timeline_event(
        ENC_ROOM,
        json!({
            "type": "m.room.encrypted",
            "room_id": ENC_ROOM,
            "event_id": "$undec_unrelated",
            "sender": ALICE_MXID,
            "origin_server_ts": 2_000_000,
            "content": serde_json::to_value(&bogus_cipher).unwrap()
        }),
    );

    bot.inner()
        .sync_once(SyncSettings::default().timeout(Duration::ZERO))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    assert_eq!(xmsg.sent.lock().unwrap().len(), 0);
    assert!(
        hs.sent_events_in_room(ENC_ROOM).is_empty(),
        "Unrelated undecryptable event must stay silent (no 🔒 reaction)"
    );

    // 2. Inject an undecryptable reply to $bot_post_1 -> MUST react with 🔒 and NOT forward
    let reply_cipher = RoomEncryptedEventContent::new(
        EncryptedEventScheme::MegolmV1AesSha2(
            MegolmV1AesSha2ContentInit {
                ciphertext: "bogus_ciphertext_2".to_string(),
                sender_key: "bogus_sender_key".to_string(),
                device_id: "UNKNOWN_DEV".into(),
                session_id: "unknown_session_2".to_string(),
            }
            .into(),
        ),
        Some(EncryptedRelation::Reply(RelationReply::new(
            InReplyTo::new(
                <&matrix_sdk::ruma::EventId>::try_from("$bot_post_1")
                    .unwrap()
                    .to_owned(),
            ),
        ))),
    );
    hs.inject_timeline_event(
        ENC_ROOM,
        json!({
            "type": "m.room.encrypted",
            "room_id": ENC_ROOM,
            "event_id": "$undec_reply_to_bot",
            "sender": ALICE_MXID,
            "origin_server_ts": 2_005_000,
            "content": serde_json::to_value(&reply_cipher).unwrap()
        }),
    );

    bot.inner()
        .sync_once(SyncSettings::default().timeout(Duration::ZERO))
        .await
        .unwrap();
    for _ in 0..20 {
        if !hs.sent_events_in_room(ENC_ROOM).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        xmsg.sent.lock().unwrap().len(),
        0,
        "Undecryptable reply must never be forwarded"
    );
    let reactions = hs.sent_events_in_room(ENC_ROOM);
    assert_eq!(
        reactions.len(),
        1,
        "Bot must send 🔒 reaction to undecryptable reply"
    );
    assert_eq!(reactions[0].0, "m.reaction");
    assert_eq!(
        reactions[0].1["m.relates_to"]["event_id"].as_str(),
        Some("$undec_reply_to_bot")
    );
    assert_eq!(reactions[0].1["m.relates_to"]["key"].as_str(), Some("🔒"));

    // 3. Verify fetch_history and fetch_event return EventMessage with is_undecryptable() == true
    //    and format_context_line_with_screening emits UndecryptablePlaceholderLine with event_id and ts
    let fetched_single = bot
        .fetch_event(ENC_ROOM, "$undec_unrelated")
        .await
        .unwrap()
        .expect("fetch_event must return placeholder EventMessage for undecryptable event");
    assert!(fetched_single.is_undecryptable());
    assert_eq!(fetched_single.event_id, "$undec_unrelated");
    assert_eq!(fetched_single.sender_mxid, ALICE_MXID);
    assert_eq!(fetched_single.timestamp_ms, 2_000_000);

    let line = format_context_line_with_screening(
        &fetched_single,
        &[ALICE_MXID.to_string()],
        Some(OWNER_MXID),
        None,
        None,
    );
    let parsed_placeholder: UndecryptablePlaceholderLine = serde_json::from_str(&line).unwrap();
    assert_eq!(parsed_placeholder.event_id, "$undec_unrelated");
    assert_eq!(parsed_placeholder.sender, ALICE_MXID);
    assert!(!parsed_placeholder.ts.is_empty());
    assert!(parsed_placeholder.undecryptable);

    // 4. Own undecryptable bot message: redact and edit check unencrypted sender only (§5.4 / P7)
    hs.inject_timeline_event(
        ENC_ROOM,
        json!({
            "type": "m.room.encrypted",
            "room_id": ENC_ROOM,
            "event_id": "$bot_old_undec",
            "sender": BOT_MXID,
            "origin_server_ts": 2_010_000,
            "content": serde_json::to_value(&bogus_cipher).unwrap()
        }),
    );
    store
        .record_forwarded_message(&ForwardedMessageRecord {
            message_id: "01M36FWD_OWN".to_string(),
            room_id: ENC_ROOM.to_string(),
            thread_root_id: "$bot_post_1".to_string(),
            event_id: "$bot_post_1".to_string(),
            sender_mxid: ALICE_MXID.to_string(),
            addressed: true,
            sent_at: 1_000,
            ack_reaction_id: None,
            timeout_dm_sent: false,
            reply_count: 0,
        })
        .unwrap();
    store
        .record_bot_reply_event(&BotReplyEventRecord {
            reply_message_id: "01M36REP_OLD_UNDEC".to_string(),
            room_id: ENC_ROOM.to_string(),
            event_id: "$bot_old_undec".to_string(),
            thread_root_id: "$bot_post_1".to_string(),
            author_principal: "claude@sini.bitstream".to_string(),
            answered_mxid: ALICE_MXID.to_string(),
            created_at: 2_010,
        })
        .unwrap();
    store
        .set_thread_cursor(
            "$bot_post_1",
            "$bot_post_1",
            1_000,
            Some("claude@sini.bitstream"),
        )
        .unwrap();

    let mut inbox = MockInbox::new("sini.bitstream");
    let redact_own_del = SvcDelivery {
        message_id: "01M36RED_OWN".to_string(),
        from_name: "claude@sini.bitstream".to_string(),
        text: json!({ "redact": "01M36REP_OLD_UNDEC", "reason": "stale" }).to_string(),
        envelope: String::new(),
        in_reply_to: Some("01M36FWD_OWN".to_string()),
        origin: Some(json!({ "service": "claude", "node": "sini.bitstream" })),
    };
    let out_red = handle_inbox_delivery_with_guard(
        &redact_own_del,
        &config,
        bot.as_ref(),
        &store,
        xmsg.as_ref(),
        &guard,
        &mut inbox,
        2_020,
    )
    .await
    .unwrap();
    assert_eq!(
        out_red,
        BotOutcome::Redacted,
        "Redacting own undecryptable message must succeed via unencrypted sender check"
    );
}

// =================================================================================================
// §7.3 Cells:
// - Startup table (§3.1) rows 1, 2, 3, 4
// - --reset-crypto-store with non-empty store => store untouched
// - Store-device mismatch => refuses to start
// - Missing store passphrase => refuses to start
// =================================================================================================
#[tokio::test]
async fn m36_cells_startup_table_passphrase_and_mismatch() {
    let server = MockServer::start().await;
    let hs = StatefulMatrixHomeserver::new();
    Mock::given(wiremock::matchers::any())
        .respond_with(hs.clone())
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();

    // Missing / empty store passphrase => refuses to start
    let cfg_no_pass = Config {
        store_passphrase_file: None,
        ..make_config(&server.uri(), tmp.path())
    };
    let err_no_pass = cfg_no_pass.load_store_passphrase().unwrap_err();
    assert!(
        err_no_pass.to_string().contains("store_passphrase_file"),
        "Expected missing passphrase error, got: {err_no_pass}"
    );
    let err_empty_pass = MatrixSdkClient::new_with_e2ee(
        &server.uri(),
        BOT_MXID,
        "tok_bot_1",
        &tmp.path().join("empty_pass"),
        "   ",
        false,
        None,
    )
    .await
    .err()
    .expect("Empty passphrase must refuse to start");
    assert!(err_empty_pass
        .to_string()
        .contains("store passphrase is empty"));

    // Row 3: First boot (empty store, no server device keys, no server cross-signing) -> bootstraps
    let store_dir = tmp.path().join("bot_store");
    assert!(is_crypto_store_empty(&store_dir).unwrap());
    let recovery_key = "m36-recovery-passphrase-secret";
    let bot1 = MatrixSdkClient::new_with_e2ee(
        &server.uri(),
        BOT_MXID,
        "tok_bot_1",
        &store_dir,
        "bot-store-passphrase",
        false,
        Some(recovery_key),
    )
    .await
    .unwrap();
    let ed_key_initial = bot1.inner().encryption().ed25519_key().await.unwrap();
    assert!(!is_crypto_store_empty(&store_dir).unwrap());
    drop(bot1);

    // Row 4a: Restart with non-empty store and same token -> keeps same device & ed25519 key
    let bot2 = MatrixSdkClient::new_with_e2ee(
        &server.uri(),
        BOT_MXID,
        "tok_bot_1",
        &store_dir,
        "bot-store-passphrase",
        false,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        bot2.inner().encryption().ed25519_key().await,
        Some(ed_key_initial.clone())
    );
    drop(bot2);

    // Row 4b: --reset-crypto-store with non-empty store => store is untouched!
    let bot_reset_nonempty = MatrixSdkClient::new_with_e2ee(
        &server.uri(),
        BOT_MXID,
        "tok_bot_1",
        &store_dir,
        "bot-store-passphrase",
        true,
        Some("unused-recovery-key"),
    )
    .await
    .unwrap();
    assert_eq!(
        bot_reset_nonempty.inner().encryption().ed25519_key().await,
        Some(ed_key_initial),
        "--reset-crypto-store on a non-empty store must leave the store untouched"
    );

    // Row 4c: Store-device mismatch (non-empty store has MATRIX_XMSG, token has MATRIX_XMSG_2) => refuses!
    drop(bot_reset_nonempty);
    let mismatch_err = MatrixSdkClient::new_with_e2ee(
        &server.uri(),
        BOT_MXID,
        "tok_bot_2",
        &store_dir,
        "bot-store-passphrase",
        false,
        None,
    )
    .await
    .err()
    .expect("Store-device mismatch must refuse to start");
    assert!(
        mismatch_err
            .to_string()
            .contains("account in the store doesn't match")
            || mismatch_err
                .to_string()
                .contains("Failed to restore E2EE session"),
        "Unexpected mismatch error: {mismatch_err}"
    );

    // Have Alice send a pre-loss encrypted message to ENC_ROOM so MATRIX_XMSG backs up the Megolm room key
    let bot_pre_loss = MatrixSdkClient::new_with_e2ee(
        &server.uri(),
        BOT_MXID,
        "tok_bot_1",
        &store_dir,
        "bot-store-passphrase",
        false,
        None,
    )
    .await
    .unwrap();
    let alice = build_alice_client(&server.uri(), &tmp.path().join("alice_store")).await;
    alice.sync_once(SyncSettings::default()).await.unwrap();
    bot_pre_loss
        .inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    alice.sync_once(SyncSettings::default()).await.unwrap();
    let enc_room_alice = alice
        .get_room(RoomId::parse(ENC_ROOM).unwrap().as_ref())
        .unwrap();
    let pre_loss_ev_id = enc_room_alice
        .send(RoomMessageEventContent::text_plain(
            "@genie:hs1.test pre-loss message for backup",
        ))
        .await
        .unwrap()
        .response
        .event_id
        .to_string();
    bot_pre_loss
        .inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    bot_pre_loss
        .inner()
        .encryption()
        .backups()
        .wait_for_steady_state()
        .await
        .unwrap();
    assert!(
        !hs.state.lock().unwrap().backup_keys.is_empty(),
        "Bot must have uploaded the Megolm room key to server backup"
    );
    drop(bot_pre_loss);

    // Row 1: Empty store + old token (server already has device keys for MATRIX_XMSG) => refuses BEFORE restore_session!
    let lost_store_dir = tmp.path().join("lost_store");
    let uploads_before = hs.state.lock().unwrap().keys_upload_count;
    let row1_err = MatrixSdkClient::new_with_e2ee(
        &server.uri(),
        BOT_MXID,
        "tok_bot_1",
        &lost_store_dir,
        "bot-store-passphrase",
        true,
        Some(recovery_key),
    )
    .await
    .err()
    .expect("Row 1 (empty store + server has device keys) must refuse always");
    let uploads_after = hs.state.lock().unwrap().keys_upload_count;
    assert_eq!(
        uploads_before, uploads_after,
        "Row 1 refusal must run BEFORE restore_session with zero key uploads"
    );
    assert!(
        is_crypto_store_empty(&lost_store_dir).unwrap(),
        "Row 1 refusal must run BEFORE restore_session so the empty crypto store is never populated"
    );
    assert!(
        row1_err.to_string().contains("mint a new device token"),
        "Expected 'mint a new device token' in Row 1 error, got: {row1_err}"
    );

    // Row 2 without --reset-crypto-store: Empty store + new token (MATRIX_XMSG_2) + cross-signing on server => refuses!
    let row2_err = MatrixSdkClient::new_with_e2ee(
        &server.uri(),
        BOT_MXID,
        "tok_bot_2",
        &lost_store_dir,
        "bot-store-passphrase",
        false,
        None,
    )
    .await
    .err()
    .expect("Row 2 without --reset-crypto-store must refuse");
    assert!(
        row2_err
            .to_string()
            .contains("pass --reset-crypto-store to recover"),
        "Expected '--reset-crypto-store' in Row 2 error, got: {row2_err}"
    );

    // Row 2 with --reset-crypto-store + recovery_key: recovers cross-signing from SSSS, signs MATRIX_XMSG_2,
    // downloads room keys from backup on decryption failure, and decrypts the pre-loss message!
    let bot_recovered = MatrixSdkClient::new_with_e2ee(
        &server.uri(),
        BOT_MXID,
        "tok_bot_2",
        &lost_store_dir,
        "bot-store-passphrase",
        true,
        Some(recovery_key),
    )
    .await
    .expect("Row 2 with --reset-crypto-store and recovery_key must succeed");

    // Verify Alice sees MATRIX_XMSG_2 cross-signed by owner
    alice.sync_once(SyncSettings::default()).await.unwrap();
    let bot_uid = matrix_sdk::ruma::UserId::parse(BOT_MXID).unwrap();
    let bot_devs = alice.encryption().get_user_devices(&bot_uid).await.unwrap();
    let dev2 = bot_devs
        .get(<&DeviceId>::from("MATRIX_XMSG_2"))
        .expect("Alice must see MATRIX_XMSG_2");
    assert!(
        dev2.is_cross_signed_by_owner(),
        "Recovered device MATRIX_XMSG_2 must be cross-signed by owner"
    );

    // Verify bot_recovered decrypts the pre-loss message from server backup via AfterDecryptionFailure
    bot_recovered
        .inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    let mut decrypted_pre_loss = false;
    for _ in 0..10 {
        if let Some(fetched) = bot_recovered
            .fetch_event(ENC_ROOM, &pre_loss_ev_id)
            .await
            .unwrap()
        {
            if fetched.body.contains("pre-loss message for backup") {
                decrypted_pre_loss = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        decrypted_pre_loss,
        "Recovered bot must download room key from backup and decrypt pre-loss message"
    );
}

// =================================================================================================
// Live Finding (01M59E0TFZCM8J4VCFHZT96F67):
// The bot never sends plaintext into a room that has `m.room.encryption`.
// Cell: an encrypted room with encryption disabled in the client => every send path refuses,
// and nothing appears in the room.
// =================================================================================================
#[tokio::test]
async fn m36_cell_no_plaintext_fallback_in_encrypted_room() {
    let server = MockServer::start().await;
    let hs = StatefulMatrixHomeserver::new();
    Mock::given(wiremock::matchers::any())
        .respond_with(hs.clone())
        .mount(&server)
        .await;

    // Build client with encryption disabled (`MatrixSdkClient::new`)
    let unenc_bot = MatrixSdkClient::new(&server.uri(), BOT_MXID, "tok_bot_1")
        .await
        .unwrap();
    assert!(!unenc_bot.is_encryption_enabled());

    // 1. Cold room (before sync) — send_notice, edit_notice, send_reaction must all refuse
    assert!(unenc_bot
        .send_notice(ENC_ROOM, None, "plaintext attempt 1", None)
        .await
        .is_err());
    assert!(unenc_bot
        .send_notice(
            ENC_ROOM,
            Some("$root"),
            "plaintext reply attempt",
            Some(ALICE_MXID)
        )
        .await
        .is_err());
    assert!(unenc_bot
        .edit_notice(ENC_ROOM, "$orig", "plaintext edit attempt")
        .await
        .is_err());
    assert!(unenc_bot
        .send_reaction(ENC_ROOM, "$orig", "👍")
        .await
        .is_err());

    // 2. Synced room in SDK state with encryption disabled in client — every send path must still refuse!
    unenc_bot
        .inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();

    assert!(unenc_bot
        .send_notice(ENC_ROOM, None, "plaintext open_thread attempt", None)
        .await
        .is_err());
    assert!(unenc_bot
        .send_notice(ENC_ROOM, Some("$root"), "plaintext reply attempt 2", None)
        .await
        .is_err());
    assert!(unenc_bot
        .edit_notice(ENC_ROOM, "$orig", "plaintext edit attempt 2")
        .await
        .is_err());
    assert!(unenc_bot
        .send_reaction(ENC_ROOM, "$orig", "🔒")
        .await
        .is_err());

    // 3. Cached DM room that is encrypted — send_dm must refuse!
    let store = Arc::new(Store::new_in_memory().unwrap());
    store
        .set_dm_room(OWNER_MXID, ENC_ROOM, chrono::Utc::now().timestamp())
        .unwrap();
    unenc_bot.set_store(store);
    assert!(unenc_bot
        .send_dm(OWNER_MXID, "plaintext dm attempt")
        .await
        .is_err());

    // Verify NOTHING appeared in ENC_ROOM
    assert!(
        hs.sent_events_in_room(ENC_ROOM).is_empty(),
        "Zero events may be sent to an encrypted room when encryption is unavailable!"
    );
}
