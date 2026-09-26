//! OAuth 2.0 with PKCE flow implementation
//!
//! Supports all three providers with proper PKCE challenge/verifier.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::integrations::{IntegrationError, ProviderType, Result};

/// OAuth configuration for a provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthConfig {
    pub provider: ProviderType,
    pub client_id: String,
    /// Client secret (optional for PKCE flows)
    pub client_secret: Option<String>,
    pub redirect_uri: String,
    pub scopes: Vec<String>,
}

impl OAuthConfig {
    /// Google Drive OAuth config
    pub fn google_drive(
        client_id: String,
        client_secret: Option<String>,
        redirect_uri: String,
    ) -> Self {
        Self {
            provider: ProviderType::GoogleDrive,
            client_id,
            client_secret,
            redirect_uri,
            scopes: vec![
                "https://www.googleapis.com/auth/drive".into(),
                "https://www.googleapis.com/auth/drive.file".into(),
            ],
        }
    }

    /// Dropbox OAuth config
    pub fn dropbox(client_id: String, client_secret: Option<String>, redirect_uri: String) -> Self {
        Self {
            provider: ProviderType::Dropbox,
            client_id,
            client_secret,
            redirect_uri,
            scopes: vec![
                "files.content.read".into(),
                "files.content.write".into(),
                "files.metadata.read".into(),
                "files.metadata.write".into(),
            ],
        }
    }

    /// OneDrive OAuth config
    pub fn onedrive(
        client_id: String,
        client_secret: Option<String>,
        redirect_uri: String,
    ) -> Self {
        Self {
            provider: ProviderType::OneDrive,
            client_id,
            client_secret,
            redirect_uri,
            scopes: vec!["Files.ReadWrite.All".into(), "offline_access".into()],
        }
    }

    /// Get authorization endpoint for provider
    pub fn auth_endpoint(&self) -> &'static str {
        match self.provider {
            ProviderType::GoogleDrive => "https://accounts.google.com/o/oauth2/v2/auth",
            ProviderType::Dropbox => "https://www.dropbox.com/oauth2/authorize",
            ProviderType::OneDrive => {
                "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
            }
        }
    }

    /// Get token endpoint for provider
    pub fn token_endpoint(&self) -> &'static str {
        match self.provider {
            ProviderType::GoogleDrive => "https://oauth2.googleapis.com/token",
            ProviderType::Dropbox => "https://api.dropboxapi.com/oauth2/token",
            ProviderType::OneDrive => "https://login.microsoftonline.com/common/oauth2/v2.0/token",
        }
    }
}

/// OAuth provider trait for token management
pub trait OAuthProvider {
    fn oauth_config(&self) -> &OAuthConfig;
    fn oauth_config_mut(&mut self) -> &mut OAuthConfig;
}

/// OAuth token with refresh support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthToken {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_type: String,
    /// Expiration time (Unix timestamp)
    pub expires_at: Option<i64>,
    /// Original scopes
    pub scope: Option<String>,
}

impl OAuthToken {
    /// Check if token is expired (with 5 minute buffer)
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => {
                let now = chrono::Utc::now().timestamp();
                expires_at - 300 <= now // 5 minute buffer
            }
            None => false, // No expiration means it doesn't expire
        }
    }

    /// Parse from OAuth token response
    pub fn from_response(response: &TokenResponse) -> Self {
        let expires_at = response
            .expires_in
            .map(|secs| chrono::Utc::now().timestamp() + secs as i64);

        Self {
            access_token: response.access_token.clone(),
            refresh_token: response.refresh_token.clone(),
            token_type: response
                .token_type
                .clone()
                .unwrap_or_else(|| "Bearer".into()),
            expires_at,
            scope: response.scope.clone(),
        }
    }
}

/// Token response from OAuth provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_type: Option<String>,
    pub expires_in: Option<u64>,
    pub scope: Option<String>,
}

/// PKCE (Proof Key for Code Exchange) flow implementation
pub struct PkceFlow {
    pub config: OAuthConfig,
    code_verifier: String,
    code_challenge: String,
    state: String,
}

impl PkceFlow {
    /// Create new PKCE flow
    pub fn new(config: OAuthConfig) -> Self {
        let code_verifier = Self::generate_verifier();
        let code_challenge = Self::generate_challenge(&code_verifier);
        let state = Self::generate_state();

        Self {
            config,
            code_verifier,
            code_challenge,
            state,
        }
    }

    /// Generate random code verifier (43-128 chars)
    fn generate_verifier() -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
        URL_SAFE_NO_PAD.encode(&bytes)
    }

    /// Generate S256 code challenge from verifier
    fn generate_challenge(verifier: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(verifier.as_bytes());
        let hash = hasher.finalize();
        URL_SAFE_NO_PAD.encode(hash)
    }

    /// Generate random state parameter
    fn generate_state() -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..16).map(|_| rng.gen()).collect();
        URL_SAFE_NO_PAD.encode(&bytes)
    }

    /// Get the state parameter for validation
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Get the code verifier (needed for token exchange)
    pub fn code_verifier(&self) -> &str {
        &self.code_verifier
    }

    /// Build authorization URL for user redirect
    pub fn authorization_url(&self) -> String {
        let scopes = self.config.scopes.join(" ");

        let mut params = vec![
            ("client_id", self.config.client_id.as_str()),
            ("redirect_uri", self.config.redirect_uri.as_str()),
            ("response_type", "code"),
            ("scope", &scopes),
            ("state", &self.state),
            ("code_challenge", &self.code_challenge),
            ("code_challenge_method", "S256"),
        ];

        // Provider-specific parameters
        match self.config.provider {
            ProviderType::GoogleDrive => {
                params.push(("access_type", "offline"));
                params.push(("prompt", "consent"));
            }
            ProviderType::Dropbox => {
                params.push(("token_access_type", "offline"));
            }
            ProviderType::OneDrive => {
                params.push(("response_mode", "query"));
            }
        }

        let query = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        format!("{}?{}", self.config.auth_endpoint(), query)
    }

    /// Exchange authorization code for tokens
    pub async fn exchange_code(&self, code: &str, client: &reqwest::Client) -> Result<OAuthToken> {
        let mut params = vec![
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", self.config.redirect_uri.as_str()),
            ("client_id", self.config.client_id.as_str()),
            ("code_verifier", &self.code_verifier),
        ];

        if let Some(ref secret) = self.config.client_secret {
            params.push(("client_secret", secret.as_str()));
        }

        let response = client
            .post(self.config.token_endpoint())
            .form(&params)
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(IntegrationError::OAuth(format!(
                "Token exchange failed: {}",
                error_text
            )));
        }

        let token_response: TokenResponse = response
            .json()
            .await
            .map_err(|e| IntegrationError::Serialization(e.to_string()))?;

        Ok(OAuthToken::from_response(&token_response))
    }
}

/// Refresh an OAuth token
pub async fn refresh_token(
    config: &OAuthConfig,
    refresh_token: &str,
    client: &reqwest::Client,
) -> Result<OAuthToken> {
    let mut params = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", config.client_id.as_str()),
    ];

    if let Some(ref secret) = config.client_secret {
        params.push(("client_secret", secret.as_str()));
    }

    let response = client
        .post(config.token_endpoint())
        .form(&params)
        .send()
        .await
        .map_err(|e| IntegrationError::Network(e.to_string()))?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();

        if status.as_u16() == 400 || status.as_u16() == 401 {
            return Err(IntegrationError::TokenRefreshFailed(format!(
                "Refresh token invalid or revoked: {}",
                error_text
            )));
        }

        return Err(IntegrationError::TokenRefreshFailed(error_text));
    }

    let token_response: TokenResponse = response
        .json()
        .await
        .map_err(|e| IntegrationError::Serialization(e.to_string()))?;

    // Preserve original refresh token if not returned
    let mut new_token = OAuthToken::from_response(&token_response);
    if new_token.refresh_token.is_none() {
        new_token.refresh_token = Some(refresh_token.to_string());
    }

    Ok(new_token)
}

/// Validate state parameter matches
pub fn validate_state(expected: &str, received: &str) -> Result<()> {
    if expected != received {
        return Err(IntegrationError::OAuth(
            "State parameter mismatch - possible CSRF attack".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pkce_challenge() {
        let flow = PkceFlow::new(OAuthConfig::google_drive(
            "test_client".into(),
            None,
            "http://localhost:8080/callback".into(),
        ));

        // Verify challenge is S256 of verifier
        let expected = PkceFlow::generate_challenge(flow.code_verifier());
        assert_eq!(flow.code_challenge, expected);
    }

    #[test]
    fn test_token_expiry() {
        let token = OAuthToken {
            access_token: "test".into(),
            refresh_token: None,
            token_type: "Bearer".into(),
            expires_at: Some(chrono::Utc::now().timestamp() - 100), // Expired
            scope: None,
        };
        assert!(token.is_expired());

        let valid_token = OAuthToken {
            access_token: "test".into(),
            refresh_token: None,
            token_type: "Bearer".into(),
            expires_at: Some(chrono::Utc::now().timestamp() + 3600), // Valid
            scope: None,
        };
        assert!(!valid_token.is_expired());
    }
}
