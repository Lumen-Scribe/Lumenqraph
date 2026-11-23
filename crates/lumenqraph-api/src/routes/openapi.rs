//! OpenAPI documentation routes: /openapi.json, /docs, /redoc

use axum::Router;
use utoipa::OpenApi;
use utoipa_redoc::{Redoc, Servable};
use utoipa_swagger_ui::SwaggerUi;

use crate::openapi::ApiDoc;

/// Serve the Swagger UI at `/docs`. Generic over the app state so it merges into
/// the API router, which carries `AppState`.
pub fn swagger_ui<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    SwaggerUi::new("/docs/swagger-ui")
        .url("/openapi.json", ApiDoc::openapi())
        .into()
}

/// Serve the Redoc UI at `/redoc`
pub fn redoc_ui<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Redoc::with_url("/redoc", ApiDoc::openapi()).into()
}

/// OpenAPI documentation endpoints: the JSON spec, Swagger UI and Redoc.
pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    // `/openapi.json` is served by the Swagger UI router below.
    Router::new().merge(swagger_ui()).merge(redoc_ui())
}
