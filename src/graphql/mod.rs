pub mod mutations;
pub mod queries;
pub mod subscriptions;
pub mod types;

use async_graphql::Schema;

use crate::AppState;
use mutations::MutationRoot;
use queries::QueryRoot;
use subscriptions::SubscriptionRoot;

pub type AppSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

pub fn build_schema(state: AppState) -> AppSchema {
    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(state)
        .finish()
}

pub fn sdl() -> String {
    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .finish()
        .sdl()
}
