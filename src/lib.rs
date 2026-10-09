pub mod bot;
pub mod config;
pub mod context;
pub mod error;
pub mod matrix;
pub mod sender_map;
pub mod store;
pub mod xmsg;

pub use bot::{
    format_expert_reply, handle_incoming_event, handle_incoming_reaction, parse_reaction_control,
    BotOutcome, ControlAction, IncomingMatrixEvent, IncomingReactionEvent,
};
pub use config::{Admission, Config};
pub use error::AppError;
