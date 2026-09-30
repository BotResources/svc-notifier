use std::collections::BTreeSet;

use futures_util::future::BoxFuture;
use service_engine::pipeline::Reaction;

use super::aggregate::{Cause, Notification, NotificationRow};
use super::store;
use super::wire::InboundDeliver;
use crate::kernel::ReactionFault;

/// One private notification per recipient. First wins: a recipient who already has the
/// notification of this source event is skipped, whatever the payload of the resend.
/// A request that breaks a rule is refused as a whole, before any write.
pub fn deliver<'r>(
    cx: &'r mut Reaction<'r>,
    msg: InboundDeliver,
) -> BoxFuture<'r, Result<(), ReactionFault>> {
    Box::pin(async move {
        let command = msg.0;
        validate(&command)?;
        let now = cx.now().as_datetime();
        let recipients: BTreeSet<_> = command.recipient_ids.iter().copied().collect();
        for recipient in recipients {
            let id = NotificationRow::id_for(command.source_event_id, recipient);
            if cx.load::<NotificationRow>(&id).await?.is_some()
                || store::exists_for_source(cx.connection(), command.source_event_id, recipient)
                    .await?
            {
                continue;
            }
            let notification = NotificationRow::deliver(
                command.source_event_id,
                recipient,
                command.template.clone(),
                command.payload.clone(),
                command.link.as_ref().map(|link| link.as_str().to_owned()),
                now,
            );
            cx.create(&notification).await?;
            cx.impact_caused::<Notification, _>(&id, Cause::Delivered)?;
        }
        Ok(())
    })
}

fn validate(command: &br_notifier_contract::DeliverNotification) -> Result<(), ReactionFault> {
    if command.recipient_ids.is_empty() {
        return Err(ReactionFault::Refused(
            "the command names no recipient".into(),
        ));
    }
    if command.template.trim().is_empty() {
        return Err(ReactionFault::Refused("the template is empty".into()));
    }
    if let Some(offending) = command.template.chars().find(|c| c.is_control()) {
        return Err(ReactionFault::Refused(format!(
            "the template carries the control character U+{:04X}",
            offending as u32
        )));
    }
    Ok(())
}
