#!/usr/bin/node
let fluree_utils = require('@fluree/crypto-utils');
let fluree_crypto = require('@fluree/crypto-base');
let privkey = "bf8a7281f43918a18a3feab41d17e84f93b064c441106cf248307d87f8a60453"
let pub_key = fluree_crypto.pub_key_from_private(privkey);
let fs = require('fs');
let obj = JSON.parse(fs.readFileSync(0, 'utf-8'));
let body = obj["body"];
let local_digest = fluree_crypto.sha2_256_normalize(body, "base64");
let uri = obj["uri"];
let headers = obj["headers"]
let ctype = headers["Content-Type"]
let digest = headers["Digest"]
let digest_parts = digest.split("=")
let hashalgo = digest_parts.shift()
let b64hash = digest_parts.join("=")
let mydate = headers["X-Fluree-Date"]
let signature = headers["Signature"]
let signature_parts = signature.split(",")
let hexsignature = null
signature_parts.forEach(function (item, index) {
  let keyval = item.split("=");
  if (keyval[0] === "signature") {
     hexsignature = keyval[1].substring(1,keyval[1].length - 1);
  }
});
var signingString = "(request-target): post " + uri +
      "\nx-fluree-date: " + mydate +
      "\ndigest: SHA-256=" + b64hash;
if (local_digest !== b64hash) {
    console.log("FAIL: invalid digest", b64hash, "should be", local_digest);
    process.exit(1)
}
try {
    if (fluree_crypto.verify_signature(pub_key, signingString, hexsignature)) {
        console.log("OK");
    } else {
        console.log("FAIL");
    }
} 
catch (err) {
    console.log("FAIL:", err.message)
}

