# Phase 10 fixture sources

Retained evidence root: `/mnt/data/binary-alpha-evidence/phase10-public-20260914T000736Z-gtovasc1`.

Provider numeric token spelling is preserved. Deriv original raw frame strings are copied with only subscription identities replaced by synthetic labels. Pocket Option payloads reconstruct the selected retained provider fields; logger fields are omitted and `response_period` is restored to `period`. Handshake frames, account bootstrap frames, second-scale fixtures, and all faults created by tests are synthetic.

| Fixture | Origin | SHA-256 |
| --- | --- | --- |
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
