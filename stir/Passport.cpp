#include "Passport.h"

#include <folly/Range.h>
#include <folly/Format.h>
#include <folly/json.h>
#include <folly/dynamic.h>
#include <folly/Uri.h>
#include <folly/ssl/OpenSSLHash.h>
#include <folly/ssl/OpenSSLPtrTypes.h>
#include <folly/portability/GFlags.h>
#include <proxygen/lib/utils/Base64.h> // TODO: fix it or replace

#include <array>
#include <algorithm>
#include <chrono>

using folly::ssl::OpenSSLHash;
using folly::ssl::EvpPkeyUniquePtr;
using folly::ssl::EcdsaSigUniquePtr;
using folly::StringPiece;
using folly::trimWhitespace;
using proxygen::Base64;

DEFINE_uint32(valid_iat_period, 60,
              "Fail SHAKEN verification, if iat value in identity header "
              "exceeds current time by this value");


std::string makePassport(const SigningRequest &req) {
  EvpPkeyUniquePtr priv;
  EC_KEY *ec;
  FILE *pemfile;

  pemfile = fopen("/home/sergio/tmp/ec256-private.pem", "r");
  assert(pemfile);
  priv.reset(PEM_read_PrivateKey(pemfile, NULL, NULL, NULL));
  fclose(pemfile);
  ec = EVP_PKEY_get0_EC_KEY(priv.get());
  assert(priv && ec);

  const std::string x5u = "http://asipto.lab/stir/cert.pem";
  std::string header = folly::sformat
    (R"({{"alg":"ES256","ppt":"shaken","typ":"passport","x5u":"{}"}})",
     x5u);

  // Sort destinations lexicographically
  folly::fbvector<StringPiece> dest(req.dest.size());
  for (size_t i = 0; i < req.dest.size(); ++i)
    dest[i] = req.dest[i];
  std::sort(dest.begin(), dest.end());

  std::string payload = folly::sformat
    (R"({{"attest":"{}","dest":{{"tn":["{}"]}},"iat":{},"orig":{{"tn":"{}"}},"origid":"{}"}})",
     req.attest, folly::join("\",\"", dest), req.iat, req.orig, req.origid);

  std::string signingString = folly::sformat
    ("{}.{}",
     Base64::urlEncode(folly::range(header)),
     Base64::urlEncode(folly::range(payload)));

  std::array<uint8_t, 32> digest;
  std::array<uint8_t, 64> sigbuf;

  OpenSSLHash::sha256(folly::range(digest),
                      folly::range(signingString));

  {
    EcdsaSigUniquePtr sig;
    const BIGNUM *r, *s;

    sig.reset(ECDSA_do_sign(digest.data(), digest.size(), ec));
    assert(sig);

    ECDSA_SIG_get0(sig.get(), &r, &s);
    assert(r && BN_num_bytes(r) <= 32);
    assert(s && BN_num_bytes(s) <= 32);

    BN_bn2binpad(r, sigbuf.data()+00, 32);
    BN_bn2binpad(s, sigbuf.data()+32, 32);
  }

  return folly::sformat
    ("{}.{};info=<{}>",
     signingString,
     Base64::urlEncode(folly::range(sigbuf)),
     x5u);
}

static const char* NO_TN_VALIDATION = "No-TN-Validation";
static const char* TN_VALIDATION_FAILED = "TN-Validation-Failed";
static const char* TN_VALIDATION_PASSED = "TN-Validation-Passed";

// TODO: implement (code, reasonText) as enum and constructor

static VerificationResponse VF_STALE_DATE(const char* desc) {
  return {403, NO_TN_VALIDATION, "Stale Date", desc};
}

static VerificationResponse VF_INVALID_IDENTITY(const char* desc) {
  return {438, NO_TN_VALIDATION, "Invalid Identity Header", desc};
}

static VerificationResponse VF_BAD_IDENTITY_INFO(const char* desc) {
  return {436, NO_TN_VALIDATION, "Bad identity Info", desc};
}

static VerificationResponse VF_UNSUPPORTED_CREDENTIAL(bool fail, const char* desc) {
  return {437, fail ? TN_VALIDATION_FAILED : NO_TN_VALIDATION, "Unsupported credential", desc};
}

VerificationResponse verifyPassport(const VerificationRequest &request) {
  using namespace std::chrono;

  uint64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
  uint64_t timeDrift = (request.time > now) ? request.time - now : now - request.time;

  // Validate the "time" parameter value in terms of "freshness":
  // a request with a “time” value which is different by more than one minute
  // from the current time will be rejected (E3).
  if (timeDrift > FLAGS_valid_iat_period)
    return VF_STALE_DATE("Received 'time' value is not fresh.");

  // Parse the "identity" parameter value.
  StringPiece identity = request.identity;
  StringPiece identityDigest = trimWhitespace(identity.split_step(';'));

  StringPiece headerDigest = identityDigest.split_step('.');
  StringPiece payloadDigest = identityDigest.split_step('.');
  StringPiece signatureDigest = identityDigest;

  // a. full form of PASSporT is required by SHAKEN:
  //    "identity-digest" parameter of Identity header has to be parsed to validate
  //    the full form format [three data portions delimited with dot (".")].
  //    If the expected format is not matched => reject (E4).
  if (headerDigest.empty() && payloadDigest.empty())
    return VF_INVALID_IDENTITY("Identity header in compact form instead of required "
                               "by SHAKEN spec full form.");

  StringPiece identityInfo;
  identity = ltrimWhitespace(identity);
  if (identity.split_step('=') == "info") {
    identity = ltrimWhitespace(identity);
    if (identity.removePrefix('<'))
      identityInfo = trimWhitespace(identity.split_step('>'));
  }

  // c. If "info" parameter is not specified => E6.
  if (identityInfo.empty())
    return VF_BAD_IDENTITY_INFO("Missing 'info' parameter in the 'identity'.");

  // d. If the URI specified in "info" parameter is not syntactically valid => E7.
  folly::Optional<folly::Uri> identityUri;
  try {
    identityUri = folly::Uri(identityInfo);
  } catch (const std::invalid_argument& ex) {
    return VF_BAD_IDENTITY_INFO("Invalid 'info' URI.");
  }

  while (!identity.empty()) {
    StringPiece param = trimWhitespace(identity.split_step('='));
    StringPiece value = trimWhitespace(identity.split_step(';'));
    // b. If "ppt" parameter is specified and its value is not "shaken" => E5.
    if (param == "ppt" && value != "shaken")
      return VF_INVALID_IDENTITY("Identity header is received with 'ppt' parameter "
                                 "value that is not 'shaken'.");
  }

  // Decode "identity-digest" parameter value to extract from the first portion
  std::string headerText;
  folly::dynamic header;
  try {
    headerText = Base64::urlDecode(headerDigest.str());
    header = folly::parseJson(headerText);
  } catch (const folly::json::parse_error &ex) {
    return VF_BAD_IDENTITY_INFO("Unable to decode PASSporT header.");
  }
  if (!header.isObject())
    return VF_BAD_IDENTITY_INFO("PASSporT header is not a JSON object.");

  // a. If one of the mentioned claims is missing -> reject request (E9).
  if (!header.get_ptr("ppt"))
    return VF_BAD_IDENTITY_INFO("Missing 'ppt' claim in the PASSporT header.");
  if (!header.get_ptr("typ"))
    return VF_BAD_IDENTITY_INFO("Missing 'typ' claim in the PASSporT header.");
  if (!header.get_ptr("alg"))
    return VF_BAD_IDENTITY_INFO("Missing 'alg' claim in the PASSporT header.");
  if (!header.get_ptr("x5u"))
    return VF_BAD_IDENTITY_INFO("Missing 'x5u' claim in the PASSporT header.");

  // b. If extracted "typ" value is not equal to "passport" -> reject request (E11).
  if (header["typ"] != "passport")
    return VF_UNSUPPORTED_CREDENTIAL(false, "'typ' from PASSporT header is not 'passport'.");
  // c. If extracted "alg" value is not equal to "ES256" -> reject request (E12).
  if (header["alg"] != "ES256")
    return VF_UNSUPPORTED_CREDENTIAL(false, "'alg' from PASSporT header is not 'ES256'.");
  // d. If extracted "x5u" value is not equal to the URI specified in the
  //    "info" parameter of Identity header -> reject request (E10).
  if (header["x5u"] != identityInfo)
    return VF_BAD_IDENTITY_INFO("'x5u' from PASSporT header doesn’t match the "
                                "'info' parameter of identity header value.");
  // e. If extracted "ppt" is not equal to "shaken" -> reject request (E13).
  if (header["ppt"] != "shaken")
    return VF_INVALID_IDENTITY("'ppt' from PASSporT header is not 'shaken'");


  // Decode “identity-digest” parameter value to extract from the second portion
  std::string payloadText;
  folly::dynamic payload;
  try {
    payloadText = Base64::urlDecode(payloadDigest.str());
    payload = folly::parseJson(payloadText);
  } catch (const folly::json::parse_error &ex) {
    return VF_BAD_IDENTITY_INFO("Unable to decode PASSporT payload.");
  }
  if (!payload.isObject())
    return VF_BAD_IDENTITY_INFO("PASSporT payload is not a JSON object.");

  // a. On missing mandatory claims reject request (E14).
  if (!payload.get_ptr("dest"))
    return VF_INVALID_IDENTITY("Missing 'dest' mandatory claim in PASSporT payload");
  if (!payload.get_ptr("orig"))
    return VF_INVALID_IDENTITY("Missing 'orig' mandatory claim in PASSporT payload");
  if (!payload.get_ptr("attest"))
    return VF_INVALID_IDENTITY("Missing 'attest' mandatory claim in PASSporT payload");
  if (!payload.get_ptr("origid"))
    return VF_INVALID_IDENTITY("Missing 'origid' mandatory claim in PASSporT payload");
  if (!payload.get_ptr("iat"))
    return VF_INVALID_IDENTITY("Missing 'iat' mandatory claim in PASSporT payload");
  if (!payload["iat"].isInt() || payload["iat"] < 0)
    return VF_INVALID_IDENTITY("Bad 'iat' claim value in PASSporT payload");

  // b. Validate the extracted from payload "iat" claim value in terms of "freshness"
  // relative to "time" value: request with "expired" 'iat' will be rejected (E15).
  uint64_t iat = payload["iat"].asInt();
  uint64_t iatDrift = (iat > request.time) ? iat - request.time : request.time - iat;
  if (iatDrift > FLAGS_valid_iat_period)
    return VF_STALE_DATE("'iat' from PASSporT payload is not fresh.");

  // c. On invalid "attest" claim reject request (E19).
  folly::dynamic attest = payload["attest"];
  if (attest != "A" && attest != "B" && attest != "C")
    return VF_INVALID_IDENTITY("'attest' claim in PASSporT payload is not valid.");

  // d. Normalize to the canonical form the received in the "verificationRequest"
  // "from" and "to" telephone numbers (remove visual separators and leading “+”)
  // and compare them with ones extracted from the "orig" and "dest" claims of
  // PASSporT payload. If they are not identical -> reject request (E16).

  // TODO: TN normalization
  // TODO: handle exception if no "TN" present
  if (payload["orig"]["tn"] != StringPiece(request.from))
    return VF_INVALID_IDENTITY("'orig' claim from PASSporT payload doesn’t match the "
                               "received in the verification request claim.");
  if (payload["dest"]["tn"] != folly::dynamic::array(folly::range(request.to)))
    return VF_INVALID_IDENTITY("'dest' claim from PASSporT payload doesn’t match the "
                               "received in the verification request claim.");

  // 6. Dereference "info" parameter URI to a resource that contains the public key
  // of the certificate used by signing service to sign a request.
  // If there is a failure to dereference the URI due to timeout or a non-existent
  // resource, the request is rejected (E8).

  // TODO: replace this stub
  EvpPkeyUniquePtr pub;
  EC_KEY *ec;
  FILE *pemfile;

  pemfile = fopen("/home/sergio/tmp/ec256-public.pem", "r");
  assert(pemfile);
  pub.reset(PEM_read_PUBKEY(pemfile, NULL, NULL, NULL));
  fclose(pemfile);
  ec = EVP_PKEY_get0_EC_KEY(pub.get());
  assert(pub && ec);

  // 7. Validate the issuing CA. On the failure to authenticate the CA
  // (for example not valid, no root CA) request will be rejected (E17).
  if (0 /*TODO*/)
    return VF_UNSUPPORTED_CREDENTIAL(true, "Failed to authenticate CA.");

  // 8. Validate the signature of "identity" digest parameter.
  // On failure, reject the request (E18).
  EcdsaSigUniquePtr sig;
  BIGNUM *r, *s;
  sig.reset(ECDSA_SIG_new());

  std::string sigbuf = Base64::urlDecode(signatureDigest.str());
  if (sigbuf.size() == 64) {
    r = BN_bin2bn((uint8_t*)sigbuf.data(), 32, NULL);
    s = BN_bin2bn((uint8_t*)sigbuf.data()+32, 32, NULL);
  } else {
    r = BN_new();
    s = BN_new();
  }

  ECDSA_SIG_set0(sig.get(), r, s);

  std::string signingString = folly::sformat
    ("{}.{}",
     Base64::urlEncode(folly::range(headerText)),
     Base64::urlEncode(folly::range(payloadText)));

  std::array<uint8_t, 32> digest;
  OpenSSLHash::sha256(folly::range(digest),
                      folly::range(signingString));

  if (ECDSA_do_verify(digest.begin(), digest.size(), sig.get(), ec) != 1)
    return {438, TN_VALIDATION_FAILED, "Invalid Identity Header", "Signature validation failed."};
  else
    return {0, TN_VALIDATION_PASSED, "", ""};
}
