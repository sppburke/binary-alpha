# Phase 10 fixture sources

Evidence root at capture: `/mnt/data/binary-alpha-evidence/phase10-public-20260914T000736Z-gtovasc1`, deleted on 2026-09-23 after the work completed. The origin paths below are relative to it; the fixtures in this directory and their SHA-256 values are now the only copy.

Provider numeric token spelling is preserved. Deriv original raw frame strings are copied with only subscription identities replaced by synthetic labels. Pocket Option payloads reconstruct the selected retained provider fields; logger fields are omitted and `response_period` is restored to `period`. Handshake frames, account bootstrap frames, second-scale fixtures, and all faults created by tests are synthetic.

| Fixture | Origin | SHA-256 |
| --- | --- | --- |
| deriv-active_symbols-excerpt.json | Public `active_symbols` (`full`) response captured during the authorized inspection of 2026-09-14 (retained as `phase10-inspect-20260914T123527Z/deriv-active_symbols-full.json`, SHA-256 `d18148fbdd892d01c44ec4431b314057b01f283f67e024dc7d11129491f0df92`); the three rows `R_100`, `R_50` and `frxEURUSD` sliced from the raw text, so `1e-05` keeps its provider spelling; no identifiers | `466bde420e7bdc2962b50496ee69051514a8c6eb4fa36795e37cd36cf3bc9abd` |
| deriv-contracts_for-R_50.json | `targeted-public-run-01/frames.jsonl:3`; subscription IDs synthetic | `2634d0cfebcaa3d01368590ad0740ec9ecc9e3c19b616a2510d7a948aa7d769d` |
| deriv-history-R_50.json | `targeted-public-run-01/frames.jsonl:5`; subscription IDs synthetic | `46455b4f400459c9a6288ff4324fe8dac88944ed92e71613d59afcd2f3d8165b` |
| deriv-contracts_for-R_100.json | `targeted-public-run-01/frames.jsonl:7`; subscription IDs synthetic | `893c14bb6fc70d1390178fdec3891be404151444eb5a6fd9bd17e5a2f09fedc0` |
| deriv-history-R_100.json | `targeted-public-run-01/frames.jsonl:9`; subscription IDs synthetic | `1c7ee08f3a02b46e64f2ddac23a21ceff090beb043977890e15c85bc676185c3` |
| deriv-tick-R_50.json | `targeted-public-run-01/frames.jsonl:12`; subscription IDs synthetic | `afeffddb525f6888c71264d98bc4fe2f8bbc08153966852f1d0a85bdcdf8c717` |
| deriv-tick-R_100.json | `targeted-public-run-01/frames.jsonl:13`; subscription IDs synthetic | `8f190dfd376a4bc36299748a1f9708e32182a988137cb91bde0033bc988b1b3c` |
| deriv-forget.json | `targeted-public-run-01/frames.jsonl:45`; subscription IDs synthetic | `21d57d67e724f53928b507702cd64cfd222b86b1e492f4e543c3f8f20b8c3f3e` |
| deriv-rate-limit.json | `run-02/frames.jsonl:3` (verbatim raw response) | `a6f93131990ce29d915fbd334afd3741f854851bfb599c9936b2ffbdff395b56` |
| pocket-history-initial.json | `pocket-two-market-run-01/market.jsonl:4`; reconstructed provider fields | `f7849e8294d8ad315275d1bdbfdff18381975ae3456dd0ef20a57c04933cca72` |
| pocket-history-older-1.json | `pocket-two-market-run-01/market.jsonl:8`; reconstructed provider fields | `e4427556b87305cacff2680b9900c394ab7912abb5963fd47b9771188d9f0d46` |
| pocket-history-older-2.json | `pocket-two-market-run-01/market.jsonl:12`; reconstructed provider fields | `5354780fa436dd28a0763165e5f359c6df43bb4dd8d77f546563a8983f4baddb` |
| pocket-live-06.json | `pocket-two-market-run-01/market.jsonl:6`; `rows` payload | `c8a58ac51c95a52a6ded584c2a9159d3b20f5336094ffce8d0f05f3ca94ef3d2` |
| pocket-live-07.json | `pocket-two-market-run-01/market.jsonl:7`; `rows` payload | `8a9d455618f41ad7ab902e5c6ee550b6c45514ab30750052131d7109904605e6` |
| pocket-live-10.json | `pocket-two-market-run-01/market.jsonl:10`; `rows` payload | `b09285c852dfaef5b888cbc9b03a761f2de5ddcc56786428e390fc8e8fe36e20` |
| pocket-live-11.json | `pocket-two-market-run-01/market.jsonl:11`; `rows` payload | `26a1cee1e3e7cfc43568156330c8e5893c84396d51f5f29fa7b7eb03e6eb44f6` |
| pocket-live-13.json | `pocket-two-market-run-01/market.jsonl:13`; `rows` payload | `aa51ae6281252b968b50a9604a98b88a9500e3a48f2c8d9a96d2d7882b8eb157` |
| pocket-live-14.json | `pocket-two-market-run-01/market.jsonl:14`; `rows` payload | `185c3ca19f521daf00647261cac17b08f5756d51d31f60f3742c634eeea9edee` |
| pocket-live-15.json | `pocket-two-market-run-01/market.jsonl:15`; `rows` payload | `01b1a9ae84e1ba25a36b783a29557bca200f16780658216391d6d5e5b3d902df` |
| pocket-live-16.json | `pocket-two-market-run-01/market.jsonl:16`; `rows` payload | `9bd933a07e3b85e1dcd499b62aeb1dadf4c30894dca946940020ec200a87f87c` |
| pocket-live-18.json | `pocket-two-market-run-01/market.jsonl:18`; `rows` payload | `a20ab5c9d62486a414c3b24fe794ceffc6b127b1b6d97e2de506b20dae22fb28` |
| pocket-live-19.json | `pocket-two-market-run-01/market.jsonl:19`; `rows` payload | `3245f9d89d71b38e095b9ec6dd12c103ae18dbb63cf7e4dbff64d54950dceb7b` |
| pocket-live-20.json | `pocket-two-market-run-01/market.jsonl:20`; `rows` payload | `254c342f9708d99094e4a8fe0fae160f24b84a7d139eecfb573f22143dd829e7` |
| pocket-live-21.json | `pocket-two-market-run-01/market.jsonl:21`; `rows` payload | `7cc6162cc5726808e817ffec9aa5c24cc0d071c16ce4c454cb481208b0d514f5` |
| pocket-live-22.json | `pocket-two-market-run-01/market.jsonl:22`; `rows` payload | `f130b55f229ad45a1c783de3da72e2464884715c30489fbbea488e27e00bef29` |
| pocket-live-23.json | `pocket-two-market-run-01/market.jsonl:23`; `rows` payload | `9e4e61655209531ca53f190e52352c024866aa02f8ca66c22af0cdda533182b7` |
| pocket-live-24.json | `pocket-two-market-run-01/market.jsonl:24`; `rows` payload | `a37bcce45cefd0f84d16265f93cc9f90f2524fc809d3fc02c7d2baec8fddb1d3` |
| pocket-live-25.json | `pocket-two-market-run-01/market.jsonl:25`; `rows` payload | `295cb807b4c6e1da85adb80846999ad67ec9e44cd0c437ca419c14ebc5aaceef` |
| pocket-live-26.json | `pocket-two-market-run-01/market.jsonl:26`; `rows` payload | `0e63a6bffdfe41d9486bfecbec8c1d0defeddef9ecce598a689171ffeb0e7fc5` |
| pocket-live-27.json | `pocket-two-market-run-01/market.jsonl:27`; `rows` payload | `6c1c1c4fe4fad1695e00a94bb8f2d8fd75e7d0dbb9d5bdb6ee75f75408cfcb43` |
| pocket-live-28.json | `pocket-two-market-run-01/market.jsonl:28`; `rows` payload | `7bb37583d908334bd1f3ea0bae5094b616ad44ce4f7ba3723f87a5551057605e` |
| pocket-live-29.json | `pocket-two-market-run-01/market.jsonl:29`; `rows` payload | `b2cd580fc580825c7e62e601e2b88d1ddd3a40e7bb108836482dfe692134f73a` |
| pocket-live-30.json | `pocket-two-market-run-01/market.jsonl:30`; `rows` payload | `6565867146109dd60a186815dac580c5d13688f70017940b0840e0dbddcf9938` |
| pocket-live-31.json | `pocket-two-market-run-01/market.jsonl:31`; `rows` payload | `3948edf8df5c577ee6ad18882ed9b30102991acbec395c8a6e9d957d38252cc4` |
| pocket-live-32.json | `pocket-two-market-run-01/market.jsonl:32`; `rows` payload | `077f2f76560aba0c20ee6a3f67b65023b33a3a749adf594b097a5f797413aae1` |
| pocket-live-33.json | `pocket-two-market-run-01/market.jsonl:33`; `rows` payload | `8813f092d94b45d43be496ff62b7c871772549d3ddc86b7a169e240b5133a9a6` |
| pocket-live-34.json | `pocket-two-market-run-01/market.jsonl:34`; `rows` payload | `5ff4af46e72a116218412893337cb7865903fcc6ffad9e4c5ef90014555885a8` |
| pocket-live-35.json | `pocket-two-market-run-01/market.jsonl:35`; `rows` payload | `0f6ad956f0cc94b80b1c84b8a6e4071d2299ef78a3dd4480eb9281c2839da82f` |
| pocket-live-36.json | `pocket-two-market-run-01/market.jsonl:36`; `rows` payload | `1bda7631800760bdbc28a8c8014963dca5cb1f35d687e9e01f5292ceecf03f53` |
| pocket-opening.txt | Synthetic opening; probe handshake contract | `c6751cc357b1ba6ef80849dd721ed1f12eb43c3ba14ce3d2fcef4907bf2ddc5e` |
| pocket-connected.txt | Synthetic namespace acknowledgement; probe handshake contract | `8b8e8130adecaa0ff6a6edf245013a180fc4e10105142f0800ee3f8c52c3f541` |
| pocket-authenticated.txt | Synthetic authentication acknowledgement; probe handshake contract | `3108f01ff0df4618129ee01e02b0ccb1dfca25758b0d940d03386142431e669c` |
| pocket-account-class.txt | Synthetic account class; probe handshake contract | `7085f8869ea334fb71db9d86e4278285b95ec25149e80366496a10fce4bccc58` |
| pocket-assets.json | Synthetic 19-element rows; symbol position 1 from retained probe | `816bd8fab7a30b8d186fcb41302eea6acd1188ca481314da3387cea1a3bba9e1` |
| history_unknown_broker.toml | Synthetic configuration rejection | `ee117374bc22f49c916611040fd3d06cd489f4d56e42964223e46b86109dd0d8` |
| unknown_broker_field.toml | Synthetic configuration rejection | `e9e6dc224a7f5fa3303b92aa1236dde2cdc27eeed768325ec121b9b20e88558f` |

Pocket mapping source identities: private source capture SHA-256 `2050c3e9ff9de02aad244bac908d4e5de73725e8ed78da3d1a3e6dc3dfbf808e`; captured provider script SHA-256 `fd85316b02dd03bb2afa797b4f9b9dfb559b17cd73e0c4bbc54783fbe70318c2`. Only the retained public market fields above are included; no original private capture is included.

## Execution fixtures

These response bodies are extracted from the retained records listed below. Numeric token text is unchanged. Account/login and application identifiers are synthetic; string proposal and subscription identities use consistent `fixture-id-N` replacements (including echoed purchases). No authenticated address or credential is retained. Source hashes cover the original JSONL record without its newline; fixture hashes include their trailing newline. Portfolio, fault, missing-field, scope-mismatch and fee cases constructed in the test are explicitly synthetic.

| Fixture | Retained source | Source SHA-256 | Fixture SHA-256 |
| --- | --- | --- | --- |
| `deriv-execution-balance-before.json` | `deriv-demo-run-01/events.jsonl:8` | `869f8fda68bb45385a6bd1096da176535e98e84ca5da9fd0d0d40266aa3973c7` | `fb153c86fc8607d9bc628189372f6214bffe446ffebb69aecd53b17c7970ea6c` |
| `deriv-execution-transaction-ack.json` | `deriv-demo-run-01/events.jsonl:12` | `1541c3e4bdbd18c7b165e4a5290c7e9436a9136b2c1d54216bbc635075f30f34` | `0a42c20db4595d0d3c87c57cb899ed227fec27b7afeec25855af8805ddc726d3` |
| `deriv-execution-proposal-call.json` | `deriv-demo-run-01/events.jsonl:14` | `20ccd4762b88581aead53d1d1c6b0decc1de1075b336f574be23e27f7551fec4` | `d3222e6093fe2af8f9529c0511960ca250beb626fd4efa7e86a628a126e983ed` |
| `deriv-execution-buy-call.json` | `deriv-demo-run-01/events.jsonl:18` | `0d98ca2f2d7046ac112422779380e5ed30faa2e8b9931c17327a3c496722dbfb` | `a3bfa5a3fe02f3411f39f3732442a652e97ac47286c5abf9b8a4a2b84e940b82` |
| `deriv-execution-transaction-buy-call.json` | `deriv-demo-run-01/events.jsonl:21` | `3b89dfc5c183c863f2f90fcb31ef1a37424d582c2e0eaf204298a64826401530` | `dc2dced4f407e4722434975b52bf74451c6d65064b5d9de9fc88e5e88ea9d51a` |
| `deriv-execution-open-call.json` | `deriv-demo-run-01/events.jsonl:22` | `1c5f1d311840c3885453a7c78b09a8afadcae4bff0a388bc8d784f39c167390b` | `c2c75a9be36ad8aff60c02c6770ab480e5c9bb19c50f245f66ee6f3888e4cbb3` |
| `deriv-execution-proposal-put.json` | `deriv-demo-run-01/events.jsonl:24` | `9dfbff0beee557dfc04f7f38b7478b72b9833c61b1b0ca01f1577a93c869c671` | `e5a498821e2d2d828fd7a014c5a4b1894edee706158f202c29bf323f68cf7e6d` |
| `deriv-execution-buy-put.json` | `deriv-demo-run-01/events.jsonl:28` | `c3d7c78d7d203b633a0d8a49e047277d8eea0da638c795985d4a066311bfa4f0` | `68af78aef4cd0f29620300d5f1778855cc09f58d5024e4c94e6e11a64a7316c2` |
| `deriv-execution-transaction-buy-put.json` | `deriv-demo-run-01/events.jsonl:31` | `238328007cc13aaace81285297eb8b5676dbee293ec18e946c85010af1b04fc7` | `7492a0ea3411ae33b6bf7b3aa79ac87aa59ce560ecdd1977e3b98b5f8bfc95c0` |
| `deriv-execution-entry-call.json` | `deriv-demo-run-01/events.jsonl:32` | `9832cacdf542c38870595abd54dc3be4a6775a6528501e9acb251ef7467d0019` | `aae7d32e169c7b1033fc1a889f53d81bdad4d3407e9bf721ea1efbbfd9cde350` |
| `deriv-execution-open-put.json` | `deriv-demo-run-01/events.jsonl:33` | `4059f8c2374f061c54b66b074e2e87813abc975f16aabaf9056ebebadd96d21e` | `0d099ed2d03329de984e2f795994dbe8c090407339f154fc54c976a805516be4` |
| `deriv-execution-entry-put.json` | `deriv-demo-run-01/events.jsonl:34` | `1ffa939e0418ea86c8d41dcc407b2508a93425008a976265bad21f24fba1ff96` | `5986df14d765508ca4a2ed7786d121805c2de66a281d5f5b653483be730a80f0` |
| `deriv-execution-transaction-win.json` | `deriv-demo-run-01/events.jsonl:46` | `f12f94d8410d9f733aad8ed11f64e1c3e202ddc25ebbd701f006dc4e5d9b70ee` | `aa8b00edb7ecf22c276a28ac923a4de03638d7e831fbbd46157d5bdea8fb4ac2` |
| `deriv-execution-won.json` | `deriv-demo-run-01/events.jsonl:47` | `edc2d11081c3e7b855763ae3b565cc63ed8f6e5840eef2aba2a53a044d8cc17c` | `8bc167a77588008ef20a5e135d1fababea22aed572bc6b199f86a2733a99e042` |
| `deriv-execution-transaction-zero.json` | `deriv-demo-run-01/events.jsonl:49` | `722adeb74cdb3b37c090a72d5205ace467348a5c587298b2083a0e9e9b50cfcc` | `3008332737ddc0cfe3deb5b4ace2c4da3f70ccda946b435633f78dce613f9891` |
| `deriv-execution-lost.json` | `deriv-demo-run-01/events.jsonl:50` | `a07794be20d754cc646c5b5d9e2cedc3ccc5f6f4cd211543954f0b4e3be58c4a` | `5c6b68ae2ec8c363a4495a9ebb68ed89578774b0c583ed9750e769a2828eca49` |
| `deriv-execution-balance-after.json` | `deriv-demo-run-01/events.jsonl:54` | `af6ae443fd9646d4c5e9a73daae7e1bbcb1b8f0dc13b381f61dd743a41a82750` | `19b2ca49dfada5ba019e3ab45ceb0b220730443f5c11e9634d443a7a07bafcd8` |
| `deriv-execution-statement-three.json` | `deriv-statement-boundary-run-01/events.jsonl:8` | `46a5f9afb39ab7aaded49b63760994ad2ea039315ba919c7040ffcd1b898116e` | `b324e3fc98d2e8bc7bf5346af2f512506db382885c95278c3aae1a33a22d50f5` |
| `deriv-execution-statement-four.json` | `deriv-statement-boundary-run-01/events.jsonl:10` | `10a71e2ee11dd3cc814fadf9b0530e684d1f73ca155da8c4f1816e26e10fc5c0` | `fb2c1f9cc8443a1d60706888bfd043d5db864f03620e1dfd4ac05fe872262e1a` |
| `deriv-execution-proposal-later.json` | `targeted-quotes-run-01/frames.jsonl:7` | `d1f94ff21df3f8635fc1a46c3f39d0f3b8b10a9d387c55ba1efc4cdf520c07ff` | `c3b1d17096c8a6f4300fcb58e23ddceb7805a23d0371eb233fa8bafd7371cd69` |

## Fixed normalized expectations

Integer microsecond/price-unit vectors below were calculated independently with exact base-ten arithmetic from the pinned history/tick and Pocket live fixtures above. Deriv live times add the test’s synthetic 0/2/20-second shifts. Pocket times subtract the declared 120-minute offset. History vectors apply the binary test’s fixed bounds; AAPL history is the test’s explicitly synthetic three-row page. These are expected normalized outputs, not additional provider observations.

| Expected fixture | Rows | SHA-256 |
| --- | --- | --- |
| `expected-history-R_50.csv` | 100 | `bb10b78dc6b1fc39f526be7aef830783161c63280aef9b31fb06e9759997d44f` |
| `expected-live-R_50.csv` | 3 | `743b16ff8c50033503563980240537bec5b4b1a4b85da1b29a2c00f116ca435d` |
| `expected-history-R_100.csv` | 100 | `8474ded99e6d4c2d29a96b71946f89adc87d412dc5613cb09f30c0706cc5b0d9` |
| `expected-live-R_100.csv` | 3 | `1c6c846f15bfa90118aaf3b8b0d037ade3bee5c6d486ab47b7e0e9cf5c29ab40` |
| `expected-history-EURUSD_otc.csv` | 1478 | `907a0900b91c48eaffaca4146d034b6d4503bea7207963bd906a2b6e48f11399` |
| `expected-history-AAPL_otc.csv` | 3 | `0b9a388f0ffeb288da0e1f414efd9b17aee5f87a04dea92acd2ed29e35fdf48a` |
| `expected-live-EURUSD_otc.csv` | 23 | `47ab2ca840051a5dff716a4537b9a8c6beb917fba30a9c32921e06521fd57703` |
| `expected-live-AAPL_otc.csv` | 4 | `1eb0f17e0e1e8c4705c483aff47f993e56f5796654edff6cb80ceb4438deaf89` |

`execution-definition.toml` is the synthetic single-CALL broker-authoritative configuration used directly by `phase10_execution.rs`; it does not name real data. The optional PUT test extends this definition.

`deriv-execution-portfolio.json` is synthetic from the pinned portfolio schema and uses `underlying_symbol`. `pocket-history-initial-period60.json` and `pocket-history-older-period60.json` are synthetic negative period fixtures, not provider observations. Deriv pagination tests cut retained history into forty-row pages with one overlapping boundary; the Pocket binary server converts retained initial numeric tokens into the pinned older-page shape. These transformations prove local paging, not additional external acceptance.
