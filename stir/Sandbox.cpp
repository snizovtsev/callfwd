#include <stir/StirApiTypes.h>

#include <folly/init/Init.h>
#include <folly/ssl/Init.h>
#include <folly/Range.h>
#include <folly/Format.h>
#include <folly/dynamic.h>
#include <folly/ssl/OpenSSLHash.h>
#include <folly/ssl/OpenSSLPtrTypes.h>
#include <proxygen/lib/utils/Base64.h>

#include <iostream>
#include <cassert>

using folly::ssl::OpenSSLHash;
using folly::ssl::EvpPkeyUniquePtr;
using folly::ssl::EcdsaSigUniquePtr;


int main(int argc, char *argv[]) {
  folly::Init init(&argc, &argv);
  folly::ssl::init();

  EvpPkeyUniquePtr priv;
  EC_KEY *ec;
  FILE *pemfile;
  // XXX: use BIO
  pemfile = fopen("/home/sergio/tmp/ec256-private.pem", "r");
  assert(pemfile);
  priv.reset(PEM_read_PrivateKey(pemfile, NULL, NULL, NULL));
  ec = EVP_PKEY_get1_EC_KEY(priv.get());
  assert(priv && ec);
  fclose(pemfile);

  SigningRequest req;
  req.attest = 'A';
  req.dest = { "493055559999" };
  req.orig = "493044448888";
  req.iat = 1614060362;
  req.origid = "4e046b69-d836-4e4a-82f2-28788829b069";

  std::string x5u = "http://asipto.lab/stir/cert.pem";
  std::string header = folly::sformat
    (R"({{"alg":"ES256","ppt":"shaken","typ":"passport","x5u":"{}"}})",
     x5u);
  std::string payload = folly::sformat
    (R"({{"attest":"{}","dest":{{"tn":["{}"]}},"iat":{},"orig":{{"tn":"{}"}},"origid":"{}"}})",
     req.attest, req.dest[0], req.iat, req.orig, req.origid);

  std::string signingString = folly::sformat
    ("{}.{}",
     proxygen::Base64::urlEncode(folly::range(header)),
     proxygen::Base64::urlEncode(folly::range(payload)));

  std::vector<uint8_t> digest(32);
  std::vector<uint8_t> sigbuf(64);

  {
    OpenSSLHash::sha256(folly::range(digest), folly::range(signingString));
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

  std::string identity = folly::sformat
    ("{}.{};info=<{}>",
     signingString,
     proxygen::Base64::urlEncode(folly::range(sigbuf)),
     x5u);

  std::cout << identity << std::endl;
}
