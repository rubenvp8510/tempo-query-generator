package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// TempoClient handles HTTP requests to Tempo search API
type TempoClient struct {
	queryEndpoint string
	tenantID      string
	token         []byte
	httpClient    *http.Client
}

// NewTempoClient creates a new Tempo client with token read from file path
// If tokenPath is empty, the client will work without authentication
func NewTempoClient(queryEndpoint, tenantID, tokenPath string) (*TempoClient, error) {
	var token []byte
	var err error

	if tokenPath != "" {
		token, err = os.ReadFile(tokenPath)
		if err != nil {
			// Log warning but continue without token
			return nil, fmt.Errorf("failed to read token from %s: %w", tokenPath, err)
		}
	}

	// Create custom transport with TLS config that allows self-signed certificates
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   time.Minute * 15,
	}

	return &TempoClient{
		queryEndpoint: queryEndpoint,
		tenantID:      tenantID,
		token:         token,
		httpClient:    httpClient,
	}, nil
}

// NewTempoClientWithToken creates a new Tempo client with token provided directly
func NewTempoClientWithToken(queryEndpoint, tenantID string, token []byte) (*TempoClient, error) {
	// Create custom transport with TLS config that allows self-signed certificates
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   time.Minute * 15,
	}

	return &TempoClient{
		queryEndpoint: queryEndpoint,
		tenantID:      tenantID,
		token:         token,
		httpClient:    httpClient,
	}, nil
}

// Search performs a Tempo search query and returns the parsed response
// startTime and endTime are optional - if both are nil, no time range is added to the query
func (c *TempoClient) Search(ctx context.Context, traceQL string, startTime, endTime *time.Time, limit int) (*TempoSearchResponse, *http.Response, error) {
	// Construct the URL: {queryEndpoint}/api/traces/v1/{tenantID}/tempo/api/search
	url := fmt.Sprintf("%s/api/traces/v1/%s/tempo/api/search", c.queryEndpoint, c.tenantID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating http request: %w", err)
	}

	// Add authentication header if token is available
	if c.token != nil {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", string(c.token)))
	}

	// Add tenant ID header for multitenancy
	if c.tenantID != "" {
		req.Header.Set("X-Scope-OrgID", c.tenantID)
	}

	// Build query parameters
	queryParams := req.URL.Query()
	queryParams.Set("q", traceQL)

	// Add time range parameters if provided
	if startTime != nil && endTime != nil {
		queryParams.Set("start", fmt.Sprintf("%d", startTime.Unix()))
		queryParams.Set("end", fmt.Sprintf("%d", endTime.Unix()))
	}

	// Set query result limit
	queryParams.Set("limit", fmt.Sprintf("%d", limit))
	req.URL.RawQuery = queryParams.Encode()

	// Make the HTTP request
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("error making http request: %w", err)
	}

	// Read response body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		res.Body.Close()
		return nil, res, fmt.Errorf("error reading response body: %w", err)
	}
	res.Body.Close()

	// Check for HTTP errors
	if res.StatusCode >= 300 {
		return nil, res, fmt.Errorf("query failed with status %d: %s", res.StatusCode, string(body))
	}

	// Parse JSON response
	var searchResp TempoSearchResponse
	if err := json.Unmarshal(body, &searchResp); err != nil {
		return nil, res, fmt.Errorf("error parsing response JSON: %w", err)
	}

	return &searchResp, res, nil
}
