use br_core_auth::Passport;
use br_core_integration::Actor;
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::principal::{Principal, PrincipalId, PrincipalResolver};
use service_engine::{PassportPrincipal, PrincipalRejected};
use sqlx::PgPool;
use uuid::Uuid;

/// The caller as the notifier sees it: the REAL ACTOR of the request.
///
/// The engine's `PassportPrincipal::from_passport` is the service's own code and the
/// engine never reads `impersonator`, so the rule lives here: a person acting as
/// another one owns the notifications of the actor, never those of the person acted
/// as. A machine identity is carried as a principal too (a `Result::Err` here would
/// answer a plain-text 401 at the transport) and refused inside every resolver.
#[derive(Debug, Clone)]
pub struct AppPrincipal {
    id: PrincipalId,
    machine: bool,
    passport: Passport,
}

impl AppPrincipal {
    pub fn from_actor(actor: Actor) -> Self {
        let passport = match actor {
            Actor::Service(id) => Passport::service(id.as_uuid(), Default::default()),
            Actor::Human(id) => Passport::human(
                id.as_uuid(),
                false,
                true,
                br_core_auth::AuthMethod::Jwt,
                None,
                Default::default(),
            ),
        };
        Self::from_resolved(passport)
    }

    fn from_resolved(passport: Passport) -> Self {
        let machine = passport.service_account_id().is_some();
        let id = match passport.user_id() {
            Some(user) => passport.impersonator_id().unwrap_or(user),
            None => passport.actor_id(),
        };
        Self {
            id: PrincipalId::from(id),
            machine,
            passport,
        }
    }

    pub fn is_machine(&self) -> bool {
        self.machine
    }

    /// The recipient whose notifications this principal owns.
    pub fn recipient(&self) -> Uuid {
        self.id.as_uuid()
    }
}

impl Principal for AppPrincipal {
    fn id(&self) -> PrincipalId {
        self.id
    }

    fn passport(&self) -> &Passport {
        &self.passport
    }
}

impl PassportPrincipal for AppPrincipal {
    fn from_passport(passport: Passport) -> Result<Self, PrincipalRejected> {
        if passport.service_account_id().is_none() && passport.user_id().is_none() {
            return Err(PrincipalRejected::new("the passport names no identity"));
        }
        Ok(Self::from_resolved(passport))
    }
}

pub struct AppPrincipalResolver;

impl PrincipalResolver<AppPrincipal> for AppPrincipalResolver {
    fn resolve<'a>(
        &'a self,
        _pg: &'a PgPool,
        current: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Option<AppPrincipal>, EngineError>> {
        Box::pin(async move { Ok(Some(current.clone())) })
    }
}
