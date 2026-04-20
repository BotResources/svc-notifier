use async_graphql::{Context, ErrorExtensions, Object, Result as GqlResult};
use uuid::Uuid;

use crate::AppState;
use crate::db;
use crate::error::AppError;

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn notifier_mark_as_read(
        &self,
        ctx: &Context<'_>,
        notification_id: Uuid,
    ) -> GqlResult<bool> {
        let state = ctx.data::<AppState>()?;
        let passport = ctx
            .data::<br_core_auth::Passport>()
            .map_err(|_| AppError::Unauthenticated.extend())?;

        let mut tx = state
            .pool
            .begin()
            .await
            .map_err(|e| AppError::from(e).extend())?;
        br_util_postgres::set_rls_context(&mut tx, passport)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to set RLS context");
                AppError::Internal.extend()
            })?;

        // Check existence (RLS filters to current user)
        if !db::notification_exists(&mut tx, notification_id)
            .await
            .map_err(|e| AppError::from(e).extend())?
        {
            return Err(AppError::NotFound("Notification not found".to_string()).extend());
        }

        // Already read -> return true (no-op)
        let _updated = db::mark_as_read(&mut tx, notification_id)
            .await
            .map_err(|e| AppError::from(e).extend())?;

        tx.commit().await.map_err(|e| AppError::from(e).extend())?;

        Ok(true)
    }

    async fn notifier_mark_all_as_read(&self, ctx: &Context<'_>) -> GqlResult<bool> {
        let state = ctx.data::<AppState>()?;
        let passport = ctx
            .data::<br_core_auth::Passport>()
            .map_err(|_| AppError::Unauthenticated.extend())?;

        let mut tx = state
            .pool
            .begin()
            .await
            .map_err(|e| AppError::from(e).extend())?;
        br_util_postgres::set_rls_context(&mut tx, passport)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to set RLS context");
                AppError::Internal.extend()
            })?;

        db::mark_all_as_read(&mut tx)
            .await
            .map_err(|e| AppError::from(e).extend())?;

        tx.commit().await.map_err(|e| AppError::from(e).extend())?;

        Ok(true)
    }

    async fn notifier_delete_notification(
        &self,
        ctx: &Context<'_>,
        notification_id: Uuid,
    ) -> GqlResult<bool> {
        let state = ctx.data::<AppState>()?;
        let passport = ctx
            .data::<br_core_auth::Passport>()
            .map_err(|_| AppError::Unauthenticated.extend())?;

        let mut tx = state
            .pool
            .begin()
            .await
            .map_err(|e| AppError::from(e).extend())?;
        br_util_postgres::set_rls_context(&mut tx, passport)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to set RLS context");
                AppError::Internal.extend()
            })?;

        let deleted = db::delete_notification(&mut tx, notification_id)
            .await
            .map_err(|e| AppError::from(e).extend())?;

        tx.commit().await.map_err(|e| AppError::from(e).extend())?;

        if !deleted {
            return Err(AppError::NotFound("Notification not found".to_string()).extend());
        }

        Ok(true)
    }
}
