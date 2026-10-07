pub mod bot;
pub mod config;
pub mod context;
pub mod error;
pub mod matrix;
pub mod sender_map;
pub mod store;
pub mod xmsg;

pub use bot::{handle_incoming_event, BotOutcome, IncomingMatrixEvent};
pub use config::Config;
pub use error::AppError;
