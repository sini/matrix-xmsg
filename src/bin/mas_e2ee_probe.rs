use matrix_sdk::{
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    encryption::{BackupDownloadStrategy, EncryptionSettings},
    ruma::{
        api::client::room::create_room::v3::Request as CreateRoomRequest,
        events::{
            room::{
                encryption::RoomEncryptionEventContent,
                message::{OriginalSyncRoomMessageEvent, RoomMessageEventContent},
            },
            InitialStateEvent,
        },
        DeviceId, RoomId, UserId,
    },
    Client, Room, SessionMeta, SessionTokens,
};
use matrix_xmsg::context::{format_context_line_with_screening, UndecryptablePlaceholderLine};
use matrix_xmsg::matrix::{is_crypto_store_empty, MatrixClient, MatrixSdkClient};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

async fn build_alice(
    homeserver_url: &str,
    store_dir: &Path,
    alice_mxid: &str,
    alice_device: &str,
    alice_token: &str,
    share_history_on_invite: bool,
) -> Result<Client, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(store_dir)?;
    let client = Client::builder()
        .homeserver_url(homeserver_url)
        .sqlite_store(store_dir, Some("alice-store-passphrase"))
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: false,
            backup_download_strategy: BackupDownloadStrategy::AfterDecryptionFailure,
            auto_enable_backups: false,
        })
        .with_enable_share_history_on_invite(share_history_on_invite)
        .build()
        .await?;

    let is_new = is_crypto_store_empty(store_dir)?;
    let uid = <&UserId>::try_from(alice_mxid)?.to_owned();
    let did = <&DeviceId>::from(alice_device).to_owned();
    let session = MatrixSession {
        meta: SessionMeta {
            user_id: uid,
            device_id: did,
        },
        tokens: SessionTokens {
            access_token: alice_token.to_string(),
            refresh_token: None,
        },
    };
    client.restore_session(session).await?;
    if is_new {
        client
            .sync_once(SyncSettings::default().timeout(Duration::ZERO))
            .await?;
        client.encryption().bootstrap_cross_signing(None).await?;
        client
            .sync_once(SyncSettings::default().timeout(Duration::ZERO))
            .await?;
    }
    Ok(client)
}

async fn send_and_await_bot_reply(
    alice: &Client,
    room_id: &RoomId,
    genie_mxid: &str,
    prompt_body: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let alice_room = alice.get_room(room_id).ok_or("Alice missing room")?;
    let received: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cl = received.clone();
    let genie_mxid_cl = genie_mxid.to_string();
    let target_room_id = room_id.to_owned();

    let handle = alice.add_event_handler(move |ev: OriginalSyncRoomMessageEvent, room: Room| {
        let received_cl = received_cl.clone();
        let genie_mxid_cl = genie_mxid_cl.clone();
        let target_room_id = target_room_id.clone();
        async move {
            if room.room_id() == target_room_id && ev.sender.as_str() == genie_mxid_cl {
                received_cl
                    .lock()
                    .unwrap()
                    .push((ev.event_id.to_string(), ev.content.body().to_string()));
            }
        }
    });

    // Clear any prior buffered sync events before sending our prompt
    alice
        .sync_once(SyncSettings::default().timeout(Duration::ZERO))
        .await?;
    received.lock().unwrap().clear();

    let send_resp = alice_room
        .send(RoomMessageEventContent::text_plain(prompt_body))
        .await?;
    let sent_ev_id = send_resp.response.event_id.to_string();
    println!("ALICE_SENT event_id={sent_ev_id} body={prompt_body:?}");

    for _ in 0..30 {
        alice
            .sync_once(SyncSettings::default().timeout(Duration::from_secs(1)))
            .await?;
        if let Some(first) = received.lock().unwrap().first().cloned() {
            alice.remove_event_handler(handle);
            return Ok((sent_ev_id, first.1));
        }
    }
    alice.remove_event_handler(handle);
    Err(format!("Timed out waiting for bot encrypted reply to {sent_ev_id}").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cmd = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "setup-room".to_string());
    let homeserver_url =
        std::env::var("HOMESERVER_URL").unwrap_or_else(|_| "https://hs1.test:8448".to_string());
    let alice_mxid = std::env::var("ALICE_MXID").unwrap_or_else(|_| "@alice:hs1.test".to_string());
    let alice_token = std::env::var("ALICE_TOKEN")?;
    let alice_device = std::env::var("ALICE_DEVICE").unwrap_or_else(|_| "ALICE_DEV".to_string());
    let genie_mxid = std::env::var("GENIE_MXID").unwrap_or_else(|_| "@genie:hs1.test".to_string());
    let base_dir = PathBuf::from(
        std::env::var("STORE_BASE_DIR").unwrap_or_else(|_| "/tmp/mas-e2ee-probe".to_string()),
    );

    let alice_dir = base_dir.join("alice");

    match cmd.as_str() {
        "setup-room" => {
            let genie_token = std::env::var("GENIE_TOKEN")?;
            let genie_device =
                std::env::var("GENIE_DEVICE").unwrap_or_else(|_| "MATRIX_XMSG".to_string());

            // 1. Verify whoami on both compatibility tokens
            let http = matrix_sdk::reqwest::Client::builder().build()?;
            let alice_whoami: serde_json::Value = serde_json::from_slice(
                &http
                    .get(format!("{homeserver_url}/_matrix/client/v3/account/whoami"))
                    .bearer_auth(&alice_token)
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?,
            )?;
            assert_eq!(alice_whoami["user_id"].as_str(), Some(alice_mxid.as_str()));
            assert_eq!(
                alice_whoami["device_id"].as_str(),
                Some(alice_device.as_str())
            );

            let genie_whoami: serde_json::Value = serde_json::from_slice(
                &http
                    .get(format!("{homeserver_url}/_matrix/client/v3/account/whoami"))
                    .bearer_auth(&genie_token)
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?,
            )?;
            assert_eq!(genie_whoami["user_id"].as_str(), Some(genie_mxid.as_str()));
            assert_eq!(
                genie_whoami["device_id"].as_str(),
                Some(genie_device.as_str())
            );
            println!(
                "WHOAMI_OK alice={}/{} genie={}/{}",
                alice_mxid, alice_device, genie_mxid, genie_device
            );

            let alice = build_alice(
                &homeserver_url,
                &alice_dir,
                &alice_mxid,
                &alice_device,
                &alice_token,
                false,
            )
            .await?;

            // Create an encrypted room and invite genie
            let enc_ev = InitialStateEvent::with_empty_state_key(
                RoomEncryptionEventContent::with_recommended_defaults(),
            );
            let invite = vec![<&UserId>::try_from(genie_mxid.as_str())?.to_owned()];
            let mut create_req = CreateRoomRequest::new();
            create_req.invite = invite;
            create_req.initial_state = vec![enc_ev.to_raw_any()];
            create_req.is_direct = false;

            let create_resp = alice.create_room(create_req).await?;
            let room_id = create_resp.room_id().to_owned();
            alice
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;
            println!("ROOM_ID={room_id}");
        }

        "verify-roundtrip-1" => {
            let room_id_str = std::env::var("ROOM_ID")?;
            let room_id = <&RoomId>::try_from(room_id_str.as_str())?;
            let alice = build_alice(
                &homeserver_url,
                &alice_dir,
                &alice_mxid,
                &alice_device,
                &alice_token,
                false,
            )
            .await?;
            alice
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;

            let prompt = format!("{genie_mxid} ping one encrypted");
            let (sent_ev_id, reply_body) =
                send_and_await_bot_reply(&alice, room_id, &genie_mxid, &prompt).await?;
            assert!(
                reply_body.contains("pong from real xmsg expert"),
                "Unexpected bot reply: {reply_body}"
            );

            // Verify raw event on wire is m.room.encrypted
            let http = matrix_sdk::reqwest::Client::builder().build()?;
            let raw_ev: serde_json::Value = serde_json::from_slice(
                &http
                    .get(format!(
                        "{homeserver_url}/_matrix/client/v3/rooms/{}/event/{}",
                        room_id, sent_ev_id
                    ))
                    .bearer_auth(&alice_token)
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?,
            )?;
            assert_eq!(raw_ev["type"].as_str(), Some("m.room.encrypted"));

            // Wait briefly for genie's key backup upload task to upload the room key to Synapse
            tokio::time::sleep(Duration::from_secs(2)).await;
            println!("PRE_LOSS_EVENT_ID={sent_ev_id}");
            println!("ROUNDTRIP_1_PASS");
        }

        "verify-restart" => {
            let room_id_str = std::env::var("ROOM_ID")?;
            let room_id = <&RoomId>::try_from(room_id_str.as_str())?;
            let alice = build_alice(
                &homeserver_url,
                &alice_dir,
                &alice_mxid,
                &alice_device,
                &alice_token,
                false,
            )
            .await?;
            alice
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;

            let prompt = format!("{genie_mxid} ping two after restart");
            let (_, reply_body) =
                send_and_await_bot_reply(&alice, room_id, &genie_mxid, &prompt).await?;
            assert!(
                reply_body.contains("pong from real xmsg expert"),
                "Unexpected bot reply after restart: {reply_body}"
            );
            println!("RESTART_ROUNDTRIP_PASS");
        }

        "verify-store-loss-and-prejoin" => {
            let room_id_str = std::env::var("ROOM_ID")?;
            let pre_loss_ev_id = std::env::var("PRE_LOSS_EVENT_ID")?;
            let genie_token_old = std::env::var("GENIE_TOKEN_OLD")?;
            let genie_token_new = std::env::var("GENIE_TOKEN_NEW")?;
            let genie_token_mismatch = std::env::var("GENIE_TOKEN_MISMATCH")?;
            let store_passphrase = std::env::var("STORE_PASSPHRASE")?;
            let recovery_key = std::env::var("RECOVERY_KEY")?;

            // 1. Missing / empty passphrase refuses to start
            let err_empty_pass = MatrixSdkClient::new_with_e2ee(
                &homeserver_url,
                &genie_mxid,
                &genie_token_new,
                &base_dir.join("empty_pass_dir"),
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
            println!("MISSING_PASSPHRASE_REFUSED_OK");

            // 2. Row 1: Empty store + old token (MATRIX_XMSG already has keys on server) => refuses!
            let row1_dir = base_dir.join("row1_empty");
            let err_row1 = MatrixSdkClient::new_with_e2ee(
                &homeserver_url,
                &genie_mxid,
                &genie_token_old,
                &row1_dir,
                &store_passphrase,
                false,
                None,
            )
            .await
            .err()
            .expect("Row 1 (empty store + existing device keys on server) must refuse");
            assert!(
                err_row1
                    .to_string()
                    .contains("server already has device keys"),
                "Unexpected Row 1 error: {err_row1}"
            );
            assert!(
                is_crypto_store_empty(&row1_dir)?,
                "Row 1 refusal must happen BEFORE restore_session touches the local store"
            );
            println!("ROW1_OLD_TOKEN_REFUSED_OK");

            // 3. Row 2 without --reset-crypto-store: Empty store + new token (MATRIX_XMSG_2) + cross-signing on server => refuses!
            let row2_dir = base_dir.join("row2_recovery");
            let err_row2_no_flag = MatrixSdkClient::new_with_e2ee(
                &homeserver_url,
                &genie_mxid,
                &genie_token_new,
                &row2_dir,
                &store_passphrase,
                false,
                None,
            )
            .await
            .err()
            .expect("Row 2 without --reset-crypto-store must refuse");
            assert!(
                err_row2_no_flag
                    .to_string()
                    .contains("pass --reset-crypto-store to recover"),
                "Unexpected Row 2 error: {err_row2_no_flag}"
            );
            assert!(is_crypto_store_empty(&row2_dir)?);
            println!("ROW2_WITHOUT_FLAG_REFUSED_OK");

            // 4. Row 2 with --reset-crypto-store + recovery_key: creates new device, cross-signs from secret storage,
            //    and restores room keys from server backup so pre-loss message decrypts!
            let recovered_bot = MatrixSdkClient::new_with_e2ee(
                &homeserver_url,
                &genie_mxid,
                &genie_token_new,
                &row2_dir,
                &store_passphrase,
                true,
                Some(&recovery_key),
            )
            .await?;
            let new_ed_key = recovered_bot
                .inner()
                .encryption()
                .ed25519_key()
                .await
                .ok_or("Missing ed25519 key on recovered device")?;
            recovered_bot
                .inner()
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;
            recovered_bot.resolve_rooms(&[room_id_str.clone()]).await?;

            // Fetch the pre-loss event from the recovered store — BackupDownloadStrategy::AfterDecryptionFailure
            // or backup recovery during recover() restores the room key from server backup!
            let mut decrypted_pre_loss = None;
            for _ in 0..10 {
                if let Some(ev) = recovered_bot
                    .fetch_event(&room_id_str, &pre_loss_ev_id)
                    .await?
                {
                    if !ev.is_undecryptable() {
                        decrypted_pre_loss = Some(ev.body);
                        break;
                    }
                }
                recovered_bot
                    .inner()
                    .sync_once(SyncSettings::default().timeout(Duration::from_millis(500)))
                    .await?;
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            let pre_loss_body = decrypted_pre_loss
                .ok_or("Recovered device failed to decrypt pre-loss message from key backup")?;
            assert!(
                pre_loss_body.contains("ping one encrypted"),
                "Unexpected pre-loss body: {pre_loss_body}"
            );
            println!("ROW2_RECOVERED_AND_BACKUP_DECRYPTED_OK");

            // Verify Alice sees genie's new device (MATRIX_XMSG_2) as cross-signed!
            let alice = build_alice(
                &homeserver_url,
                &alice_dir,
                &alice_mxid,
                &alice_device,
                &alice_token,
                true, // pin share_history_on_invite(true) for the pre-join check below
            )
            .await?;
            alice
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;
            let genie_uid = <&UserId>::try_from(genie_mxid.as_str())?;
            let new_dev_id = <&DeviceId>::from("MATRIX_XMSG_2");
            let mut is_cross_signed = false;
            for _ in 0..5 {
                alice
                    .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                    .await?;
                if let Some(dev) = alice.encryption().get_device(genie_uid, new_dev_id).await? {
                    if dev.is_cross_signed_by_owner() {
                        is_cross_signed = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            assert!(
                is_cross_signed,
                "Alice must see genie's recovered device MATRIX_XMSG_2 as cross-signed by owner"
            );
            println!("NEW_DEVICE_CROSS_SIGNED_OK");

            // 5. --reset-crypto-store on a non-empty store => store untouched!
            drop(recovered_bot);
            let untouched_bot = MatrixSdkClient::new_with_e2ee(
                &homeserver_url,
                &genie_mxid,
                &genie_token_new,
                &row2_dir,
                &store_passphrase,
                true,
                Some(&recovery_key),
            )
            .await?;
            assert_eq!(
                untouched_bot.inner().encryption().ed25519_key().await,
                Some(new_ed_key),
                "--reset-crypto-store on non-empty store must leave store untouched"
            );
            drop(untouched_bot);
            println!("RESET_ON_NON_EMPTY_UNTOUCHED_OK");

            // 6. Store-device mismatch: non-empty store (MATRIX_XMSG_2) + token for MATRIX_XMSG_3 => refuses!
            let err_mismatch = MatrixSdkClient::new_with_e2ee(
                &homeserver_url,
                &genie_mxid,
                &genie_token_mismatch,
                &row2_dir,
                &store_passphrase,
                false,
                None,
            )
            .await
            .err()
            .expect("Store-device mismatch must refuse to start");
            assert!(
                err_mismatch
                    .to_string()
                    .contains("account in the store doesn't match")
                    || err_mismatch
                        .to_string()
                        .contains("Failed to restore E2EE session"),
                "Unexpected mismatch error: {err_mismatch}"
            );
            println!("STORE_DEVICE_MISMATCH_REFUSED_OK");

            // 7. Pre-join event cell (§7.3):
            //    Alice (with_enable_share_history_on_invite(true) pinned on) creates a new encrypted room
            //    with HistoryVisibility::Shared, sends a pre-join encrypted mention BEFORE inviting genie,
            //    and then invites genie (which uploads the MSC4268 historic key bundle + sends to-device bundle info).
            //    Genie (with_enable_share_history_on_invite(false)) joins, syncs, and fetches the pre-join event:
            //    because bundle_receiver_task is disabled on genie, genie cannot decrypt the pre-join event,
            //    logs WARN, produces a placeholder with event_id and ts, and does not forward!
            use matrix_sdk::ruma::events::room::history_visibility::{
                HistoryVisibility, RoomHistoryVisibilityEventContent,
            };
            let enc_ev = InitialStateEvent::with_empty_state_key(
                RoomEncryptionEventContent::with_recommended_defaults(),
            );
            let hist_ev = InitialStateEvent::with_empty_state_key(
                RoomHistoryVisibilityEventContent::new(HistoryVisibility::Shared),
            );
            let mut create_prejoin = CreateRoomRequest::new();
            create_prejoin.initial_state = vec![enc_ev.to_raw_any(), hist_ev.to_raw_any()];
            create_prejoin.is_direct = false;
            let prejoin_room_resp = alice.create_room(create_prejoin).await?;
            let prejoin_room_id = prejoin_room_resp.room_id().to_owned();
            alice
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;

            let alice_prejoin_room = alice
                .get_room(&prejoin_room_id)
                .ok_or("Alice missing prejoin room")?;
            let prejoin_send_resp = alice_prejoin_room
                .send(RoomMessageEventContent::text_plain(format!(
                    "{genie_mxid} secret pre-join message before invite"
                )))
                .await?;
            let prejoin_ev_id = prejoin_send_resp.response.event_id.to_string();
            alice
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;

            // Alice invites genie -> because Alice has with_enable_share_history_on_invite(true),
            // Alice uploads the key bundle and sends the to-device bundle info to genie!
            alice_prejoin_room
                .invite_user_by_id(<&UserId>::try_from(genie_mxid.as_str())?)
                .await?;
            alice
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;

            // Re-open genie's client (which builds with_enable_share_history_on_invite(false))
            let genie_client = Arc::new(
                MatrixSdkClient::new_with_e2ee(
                    &homeserver_url,
                    &genie_mxid,
                    &genie_token_new,
                    &row2_dir,
                    &store_passphrase,
                    false,
                    None,
                )
                .await?,
            );
            genie_client
                .inner()
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;
            genie_client
                .resolve_rooms(&[prejoin_room_id.to_string()])
                .await?;
            genie_client
                .inner()
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;

            // Fetch the pre-join event from genie's client -> must be an undecryptable placeholder!
            let fetched_prejoin = genie_client
                .fetch_event(prejoin_room_id.as_str(), &prejoin_ev_id)
                .await?
                .ok_or("Expected placeholder EventMessage for pre-join encrypted event")?;
            assert!(
                fetched_prejoin.is_undecryptable(),
                "Pre-join event MUST remain undecryptable when with_enable_share_history_on_invite(false), got body={:?}",
                fetched_prejoin.body
            );
            let ctx_line = format_context_line_with_screening(
                &fetched_prejoin,
                &[alice_mxid.clone()],
                None,
                None,
                None,
            );
            let parsed_placeholder: UndecryptablePlaceholderLine = serde_json::from_str(&ctx_line)?;
            assert_eq!(parsed_placeholder.event_id, prejoin_ev_id);
            assert_eq!(parsed_placeholder.sender, alice_mxid);
            assert!(!parsed_placeholder.ts.is_empty());
            assert!(parsed_placeholder.undecryptable);
            println!("PREJOIN_PLACEHOLDER_OK event_id={prejoin_ev_id}");

            // 8. No-plaintext-fallback cell against real MAS+Synapse encrypted room:
            //    When encryption is disabled/unavailable in the client (MatrixSdkClient::new),
            //    every send path into the encrypted room refuses closed and sends zero events to the room!
            drop(genie_client);
            let unenc_bot =
                MatrixSdkClient::new(&homeserver_url, &genie_mxid, &genie_token_new).await?;
            unenc_bot
                .inner()
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;
            unenc_bot.resolve_rooms(&[room_id_str.clone()]).await?;

            let new_events_in_room: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let new_events_cl = new_events_in_room.clone();
            let target_room = <&RoomId>::try_from(room_id_str.as_str())?.to_owned();
            alice
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;
            let ev_handle =
                alice.add_event_handler(move |ev: OriginalSyncRoomMessageEvent, room: Room| {
                    let new_events_cl = new_events_cl.clone();
                    let target_room = target_room.clone();
                    async move {
                        if room.room_id() == target_room {
                            new_events_cl.lock().unwrap().push(ev.event_id.to_string());
                        }
                    }
                });

            assert!(
                unenc_bot
                    .send_notice(&room_id_str, None, "plaintext top-level notice attempt", None)
                    .await
                    .is_err(),
                "send_notice (top-level / open_thread) must fail closed in encrypted room when encryption is unavailable"
            );
            assert!(
                unenc_bot
                    .send_notice(
                        &room_id_str,
                        Some(&pre_loss_ev_id),
                        "plaintext thread notice attempt",
                        Some(&alice_mxid),
                    )
                    .await
                    .is_err(),
                "send_notice (threaded reply) must fail closed in encrypted room when encryption is unavailable"
            );
            assert!(
                unenc_bot
                    .edit_notice(&room_id_str, &pre_loss_ev_id, "plaintext edit attempt")
                    .await
                    .is_err(),
                "edit_notice must fail closed in encrypted room when encryption is unavailable"
            );

            alice
                .sync_once(SyncSettings::default().timeout(Duration::ZERO))
                .await?;
            alice.remove_event_handler(ev_handle);
            assert!(
                new_events_in_room.lock().unwrap().is_empty(),
                "Zero new events must appear in encrypted room when encryption is unavailable"
            );
            println!("NO_PLAINTEXT_FALLBACK_OK");
            println!("MAS_E2EE_ALL_CELLS_PASS");
        }

        _ => return Err(format!("Unknown subcommand: {cmd}").into()),
    }

    Ok(())
}
