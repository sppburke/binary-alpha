//! Phase 11 through the ordinary CLI and the real Rust owners, using only invented ticks and bars.
//! No test opens a broker, an external store, or a real protected population.
mod common;
#[path = "common/research.rs"]
mod fixture_config;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime};

use binary_alpha_engine::config::{
    Config, DataSplit, EncodingSpec, Encodings, GeneratedSearchCondition, ManifestUri,
    PortfolioGenerate, ReplayScenario, Scope, Screen, SearchCondition, StreamKey,
};
use binary_alpha_engine::dataset::coverage::CoverageRange;
use binary_alpha_engine::dataset::daily::DAY_MICROS;
use binary_alpha_engine::dataset::{DatasetRole, GenerationManifest, Layout, manifest_key};
use binary_alpha_engine::execution::{
    Comparator, Decimal, EventKind, FinancialEvent, Summary, Threshold,
};
use binary_alpha_engine::features::{FeatureManifest, FeaturePlan, ProjectionKind};
use binary_alpha_engine::portfolio::{
    Selection, SelectionManifest, State, selection_generation_id,
};
use binary_alpha_engine::research::{
    self as research, CertificationManifest, CertificationRecord, Claim, ClaimKind, Declaration,
    Frozen, Grant, Intent, Population, Receipt, Run, RunManifest, RunState, Verdict,
};
use binary_alpha_engine::search::Family;
use common::{Scratch, command};
use fixture_config::{BASE, CANDLE, CURRENCIES, HOUR, INSTRUMENTS, SCALES, SYMBOLS, time, uri};
use serde_json::Value;

const PLANTED: [u8; 4] = [0b0011_1111, 0b0000_0011, 0b0001_1111, 0b0000_1111];
const LOSING: [u8; 4] = [0b0000_0011, 0b0000_0011, 0b0000_0111, 0b0000_0111];
const PROTECTED: &str =
    "holdout data is protected; only the matching authorized certification context may open it";
/// The distinct operator account the grant command runs under.
const OPERATOR: &str = "synthetic-operator";

use fixture_config::{Row, bar_rows, recipe};
fn ticks(base: i64, rows: &[Row], instrument: usize) -> Vec<String> {
    fixture_config::ticks_at_scale(base, rows, SCALES[instrument])
}

fn decimal(text: &str) -> Decimal {
    Decimal::parse(text).unwrap()
}
fn cents(value: i64) -> Decimal {
    decimal(&format!(
        "{}{}.{:02}",
        if value < 0 { "-" } else { "" },
        value.abs() / 100,
        value.abs() % 100
    ))
}

use common::{cli, cli_as, logged_output as output};

fn logged(log: &Path) -> Vec<String> {
    fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn no_access(log: &[String], generations: &[String]) {
    for line in log {
        assert!(
            !generations.iter().any(|g| line.contains(g)),
            "unexpected access: {line}"
        );
    }
}

fn write(path: &Path, bytes: impl AsRef<[u8]>) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
}

struct Fixture {
    scratch: Scratch,
    config: Config,
    path: PathBuf,
    declaration: Declaration,
    datasets: Vec<GenerationManifest>,
}

impl Fixture {
    fn new(name: &str) -> Self {
        Self::at(Scratch::new(name), PLANTED, PLANTED)
    }

    fn at(scratch: Scratch, evaluation: [u8; 4], holdout: [u8; 4]) -> Self {
        Self::with_bars(scratch, evaluation, holdout, false)
    }

    fn bars(name: &str) -> Self {
        Self::with_bars(Scratch::new(name), PLANTED, PLANTED, true)
    }

    fn wide_split(name: &str) -> Self {
        use binary_alpha_engine::config::{NamedSearchCondition, Outputs};
        let scratch = Scratch::new(name);
        let mut config = fixture_config::configuration(&scratch.root);
        configure_bars(&mut config);
        let streams: Vec<_> = [(5, 0), (15, 5), (30, 15), (60, 30), (300, 150)]
            .map(|(duration_seconds, offset_seconds)| StreamKey {
                duration_seconds,
                offset_seconds,
            })
            .into();
        let recipe = recipe(PLANTED);
        let rows = (0..80)
            .map(|index| recipe[index % recipe.len()])
            .collect::<Vec<_>>();
        let source = scratch.path("sources/wide-bars");
        common::write_collection(
            &source,
            &(0..2)
                .map(|instrument| {
                    let divisor = 10_i64.pow(u32::from(SCALES[instrument])) as f64;
                    let bars = (0..5)
                        .flat_map(|day| {
                            let mut bars = bar_rows(BASE + day * DAY_MICROS, &rows, instrument);
                            for (index, bar) in bars.iter_mut().enumerate() {
                                let variation = (index / 60 + index % 3) as i64;
                                bar.ohlcv[1] = ((bar.ohlcv[1] * divisor).round() as i64 + variation)
                                    as f64
                                    / divisor;
                                bar.ohlcv[2] = ((bar.ohlcv[2] * divisor).round() as i64 - variation)
                                    as f64
                                    / divisor;
                            }
                            bars
                        })
                        .collect();
                    common::AssetSpec {
                        asset: SYMBOLS[instrument],
                        expected_symbol_id: Some(instrument as i32 + 7),
                        symbol_id: Some(instrument as i32 + 7),
                        files: vec![bars],
                        metadata: true,
                    }
                })
                .collect::<Vec<_>>(),
        );
        let import_path = scratch.config(
            "wide-import.toml",
            &scratch
                .bar_source()
                .replace("sources/bars", "sources/wide-bars")
                .replace("role = \"evaluation\"", "role = \"development\""),
        );
        let imports = common::current::import(&import_path).unwrap();
        let root_datasets: Vec<_> = imports
            .iter()
            .map(|line| {
                let generation = common::generation(line);
                GenerationManifest::from_json(
                    &fs::read(scratch.path("published").join(manifest_key(&generation))).unwrap(),
                )
                .unwrap()
            })
            .collect();
        assert_eq!(root_datasets.len(), 2);
        assert!(
            root_datasets
                .iter()
                .all(|root| root.layout == Some(Layout::DailyV2))
        );
        let split_root = scratch.path("split-published");
        let day =
            |first, end| CoverageRange::new(BASE + first * DAY_MICROS, BASE + end * DAY_MICROS);
        let mut split = binary_alpha_app::skeleton(&config);
        split.storage.historical_data_dir =
            serde_json::from_value(serde_json::json!(scratch.path("split-retained"))).unwrap();
        split.storage.publication_uri = format!("file://{}", split_root.display()).parse().unwrap();
        split.split = Some(DataSplit {
            namespace: "phase11-wide".into(),
            sources: root_datasets
                .iter()
                .map(|root| uri(&scratch.root, &root.generation))
                .collect(),
            development: vec![day(0, 1), day(1, 2), day(2, 3)],
            evaluation: vec![day(3, 4)],
            holdout: vec![day(4, 5)],
        });
        let split_path = scratch.path("wide-split.toml");
        write(&split_path, split.canonical_toml());
        let report = cli_as(
            &scratch.path("wide-split.log"),
            OPERATOR,
            &["data", "split", "--config", split_path.to_str().unwrap()],
        )
        .unwrap();
        let declaration_uri = report
            .lines()
            .last()
            .unwrap()
            .strip_prefix("declaration ")
            .unwrap();
        let declaration = Declaration::from_json(
            &fs::read(declaration_uri.strip_prefix("file://").unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(declaration.populations.len(), 10);
        let mut datasets = root_datasets;
        datasets.extend(declaration.populations.iter().map(|population| {
            GenerationManifest::from_json(
                &fs::read(split_root.join(manifest_key(&population.id))).unwrap(),
            )
            .unwrap()
        }));
        config.storage = split.storage;
        config.research.as_mut().unwrap().study.governance_manifest =
            declaration_uri.parse().unwrap();
        for instrument in &mut config.instruments {
            instrument.candles = streams
                .iter()
                .map(|stream| binary_alpha_engine::config::CandleSpec {
                    duration_seconds: stream.duration_seconds,
                    offset_seconds: stream.offset_seconds,
                    min_observations: Some(1),
                    hard_min_observations: Some(1),
                })
                .collect();
        }
        let research = config.research.as_mut().unwrap();
        research.portfolio.generate = Some(PortfolioGenerate { top: 8 });
        research.portfolio.members.clear();
        research.portfolio.subsets.clear();
        research.portfolio.max_policies = 64;
        research.portfolio.max_rate_age_micros = 8 * DAY_MICROS;
        research.portfolio.gates.min_decisive = Some(1);
        research.portfolio.gates.min_win_rate = Some(decimal("0.1"));
        research.portfolio.gates.max_unresolved = 100;
        research.portfolio.gates.min_profit = decimal("-1000");
        research.qualification.gates = research.portfolio.gates.clone();
        for binding in &mut research.portfolio.bindings {
            binding.alternatives.truncate(1);
        }
        let end =
            |day| time(BASE + day * DAY_MICROS + (rows.len() as i64 + 1) * CANDLE + 1_000_000);
        research.folds[0].cutoff = end(1);
        research.folds[0].decision_start = time(BASE + 2 * DAY_MICROS);
        research.folds[0].decision_end = end(2);
        research.refit.cutoff = end(2);
        research.evaluation.decision_start = time(BASE + 3 * DAY_MICROS);
        research.evaluation.decision_end = end(3);
        research.evaluation.splits = None;
        research.holdout.decision_start = time(BASE + 4 * DAY_MICROS);
        research.holdout.decision_end = end(4);
        research.holdout.splits = None;
        for index in 0..2 {
            let reference = |slot: usize| {
                format!(
                    "file://{}",
                    split_root
                        .join(manifest_key(&declaration.populations[index * 5 + slot].id))
                        .display()
                )
                .parse()
                .unwrap()
            };
            let instrument = &mut research.instruments[index];
            instrument.source_manifest = reference(0);
            instrument.features.streams = Some(streams.clone());
            instrument.features.outputs = Some(Outputs::AllSupported);
            instrument.features.moving_average_periods = Some(vec![2, 3]);
            instrument.features.rolling_window = Some(4);
            instrument.features.min_history = Some(2);
            instrument.features.price_epsilon = Some("0".into());
            instrument.features.structure = Some(serde_json::from_value(serde_json::json!({
                "swing_left":2,"swing_right":2,"rolling_windows":[2,3,4],"direction_window":2,
                "trend_efficiency_threshold":0.35,"trend_min_abs_momentum_bps":3.0,
                "range_efficiency_threshold":0.25,"compression_ratio_threshold":0.7,
                "expanded_ratio_threshold":1.3,"extreme_ratio_threshold":1.8,
                "pullback_min_trend_age":2,"trend_reset_sideways_bars":2,"failed_breakout_max_bars":2
            })).unwrap());
            instrument.features.encodings = Some(Encodings {
                max_labels: 2,
                outputs: vec![EncodingSpec {
                    output: "all_supported".into(),
                    bins: None,
                }],
            });
            instrument.search.decision_start = time(BASE);
            instrument.search.decision_end = end(0);
            instrument.search.scope = Scope::Heuristic;
            instrument.search.base_stream = streams[0];
            instrument.search.max_candidates = 100_000;
            instrument.search.max_conditions = 2;
            instrument.search.conditions = vec![
                SearchCondition::Generate(GeneratedSearchCondition {
                    stream: streams[4],
                    output: "*".into(),
                    comparator: Comparator::Eq,
                }),
                SearchCondition::Named(NamedSearchCondition {
                    stream: streams[4],
                    output: "candle_direction".into(),
                    comparator: Comparator::Eq,
                    thresholds: vec![Threshold::Text("up".into())],
                }),
            ];
            instrument.search.screen = Some(Screen {
                max_adjusted_score: 1.0,
                top: Some(64),
            });
            instrument.search.gates.min_settled = 0;
            instrument.search.gates.min_net_profit = decimal("-1000");
            instrument.search.gates.max_unresolved = 100;
            research.folds[0].inputs[index].fit_manifest = reference(1);
            research.folds[0].inputs[index].assessment_manifest = reference(2);
            research.refit.fits[index] = reference(2);
            research.evaluation.inputs[index] = reference(3);
            research.holdout.inputs[index] = reference(4);
        }
        let path = scratch.path("research.toml");
        let fixture = Self {
            scratch,
            config,
            path,
            declaration,
            datasets,
        };
        fixture.save();
        fixture
    }

    fn with_bars(scratch: Scratch, evaluation: [u8; 4], holdout: [u8; 4], bars: bool) -> Self {
        let mut config = fixture_config::configuration(&scratch.root);
        if bars {
            configure_bars(&mut config);
        }
        let mut datasets = Vec::new();
        let mut populations = Vec::new();
        let mut refs = Vec::new();
        for (hour, name, role, cells) in [
            (0, "source", DatasetRole::Development, PLANTED),
            (1, "assessment", DatasetRole::Development, PLANTED),
            (2, "refit", DatasetRole::Development, PLANTED),
            (3, "evaluation", DatasetRole::Evaluation, evaluation),
            (4, "holdout", DatasetRole::Holdout, holdout),
        ] {
            let imported = if bars {
                import_bar_pair(&scratch, name, role, BASE + hour * HOUR, &recipe(cells))
            } else {
                import_pair(&scratch, name, role, BASE + hour * HOUR, &recipe(cells))
            };
            for (i, manifest) in imported.into_iter().enumerate() {
                refs.push(uri(&scratch.root, &manifest.generation));
                populations.push(Population {
                    id: format!("{name}-{i}"),
                    role,
                    instrument: INSTRUMENTS[i].into(),
                    source: if bars {
                        "invented-five-second-bars-v1"
                    } else {
                        "invented-quarter-second-ticks-v1"
                    }
                    .into(),
                    coverage: manifest.coverage.clone(),
                    generations: vec![manifest.generation.clone()],
                    tokens: vec![format!("{name}-{i}-a"), format!("{name}-{i}-b")],
                    exposure: vec![],
                });
                datasets.push(manifest);
            }
        }
        if !bars {
            // A second real synthetic generation of the same population is a declared alias.
            // Its raw CSV differs only by an extra leading zero, preserving every observation.
            let aliases = import_pair_text(
                &scratch,
                "holdout-alias",
                DatasetRole::Holdout,
                [
                    ticks(BASE + 4 * HOUR, &recipe(holdout), 0),
                    ticks(BASE + 4 * HOUR, &recipe(holdout), 1),
                ]
                .map(|lines| {
                    lines
                        .into_iter()
                        .map(|line| line.replace("SYNTHETIC,", "SYNTHETIC,0"))
                        .collect()
                }),
            );
            for (i, alias) in aliases.into_iter().enumerate() {
                assert_ne!(alias.generation, populations[8 + i].generations[0]);
                populations[8 + i]
                    .generations
                    .push(alias.generation.clone());
                datasets.push(alias);
            }
        }
        let research = config.research.as_mut().unwrap();
        for i in 0..2 {
            research.instruments[i].source_manifest = refs[i].clone();
            research.folds[0].inputs[i].fit_manifest = refs[i].clone();
            research.folds[0].inputs[i].assessment_manifest = refs[2 + i].clone();
            research.refit.fits[i] = refs[4 + i].clone();
            research.evaluation.inputs[i] = refs[6 + i].clone();
            research.holdout.inputs[i] = refs[8 + i].clone();
        }
        let declaration = Declaration {
            schema_version: 1,
            operator: "synthetic-operator".into(),
            root: format!("file://{}/governance", scratch.root.display())
                .parse()
                .unwrap(),
            namespace: "phase11".into(),
            populations,
        };
        let path = scratch.path("research.toml");
        let fixture = Self {
            scratch,
            config,
            path,
            declaration,
            datasets,
        };
        fixture.save();
        let validated = command(&[
            "config",
            "validate",
            "--config",
            fixture.path.to_str().unwrap(),
        ])
        .unwrap();
        assert_eq!(
            validated[0],
            format!("# content-hash: {}", fixture.config.content_hash())
        );
        fixture
    }

    fn save(&self) {
        write(&self.path, self.config.canonical_toml());
        write(
            &self.scratch.path("declaration.json"),
            research::to_json(&self.declaration),
        );
    }
    fn log(&self) -> PathBuf {
        self.scratch.path("access.log")
    }
    fn published(&self) -> PathBuf {
        PathBuf::from(
            self.config
                .storage
                .publication_uri
                .to_string()
                .strip_prefix("file://")
                .unwrap(),
        )
    }
    fn run(&self) -> Result<String, String> {
        cli(
            &self.log(),
            &["research", "run", "--config", self.path.to_str().unwrap()],
        )
    }
    fn generation(&self) -> String {
        research::run_generation_id(
            &self.config.content_hash(),
            binary_alpha_app::import::CODE_REVISION,
            &self.declaration.identity(),
        )
    }
    fn manifest(&self, generation: &str) -> Value {
        serde_json::from_slice(&fs::read(self.published().join(manifest_key(generation))).unwrap())
            .unwrap()
    }
    fn object(&self, generation: &str, path: &str) -> Vec<u8> {
        let manifest = self.manifest(generation);
        let object = manifest["objects"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["path"] == path)
            .unwrap();
        fs::read(self.published().join(object["key"].as_str().unwrap())).unwrap()
    }
    fn run_record(&self) -> (RunManifest, Run) {
        let generation = self.generation();
        (
            RunManifest::from_json(
                &fs::read(self.published().join(manifest_key(&generation))).unwrap(),
            )
            .unwrap(),
            Run::from_json(&self.object(&generation, "research.json")).unwrap(),
        )
    }
    fn selection(&self, run: &Run) -> Selection {
        Selection::from_json(&self.object(&run.selection, "selection.json")).unwrap()
    }
    fn events(&self, generation: &str) -> Vec<FinancialEvent> {
        self.object(generation, "ledger/events.jsonl")
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| FinancialEvent::from_line(l).unwrap())
            .collect()
    }
    fn summary(&self, generation: &str) -> Summary {
        Summary::from_json(&self.object(generation, "summary.json")).unwrap()
    }
    fn grant_path(&self) -> PathBuf {
        self.governance_path(
            &self
                .declaration
                .key(&research::grant_key(&self.generation())),
        )
    }
    fn grant(&self) -> (String, Grant) {
        let holdout = &self.config.research.as_ref().unwrap().holdout.inputs;
        let text = cli_as(
            &self.log(),
            OPERATOR,
            &[
                "holdout",
                "grant",
                "create",
                "--config",
                self.path.to_str().unwrap(),
                "--bundle-manifest",
                &format!(
                    "file://{}",
                    self.published()
                        .join(manifest_key(&self.generation()))
                        .display()
                ),
                "--holdout-manifest",
                &holdout[0].to_string(),
                "--holdout-manifest",
                &holdout[1].to_string(),
                "--reason",
                "synthetic integration fixture",
            ],
        )
        .unwrap();
        (
            text,
            Grant::from_json(&fs::read(self.grant_path()).unwrap()).unwrap(),
        )
    }
    fn certification(&self, grant: &Grant) -> (CertificationManifest, CertificationRecord) {
        let generation = research::certification_generation_id(&self.generation(), &grant.hash);
        (
            CertificationManifest::from_json(
                &fs::read(self.published().join(manifest_key(&generation))).unwrap(),
            )
            .unwrap(),
            CertificationRecord::from_json(&self.object(&generation, "certification.json"))
                .unwrap(),
        )
    }
    /// The holdout generations and every object key they reference.
    fn protected(&self) -> Vec<String> {
        self.keys_of(|d| d.role == DatasetRole::Holdout)
    }
    /// The evaluation generations and every object key they reference.
    fn evaluation(&self) -> Vec<String> {
        let inputs = &self.config.research.as_ref().unwrap().evaluation.inputs;
        let keys = self.keys_of(|d| inputs.iter().any(|u| u.generation() == d.generation));
        assert_eq!(
            keys.iter()
                .filter(|k| inputs.iter().any(|u| u.generation() == *k))
                .count(),
            inputs.len(),
            "every evaluation generation is a fixture dataset"
        );
        keys
    }
    fn keys_of(&self, select: impl Fn(&GenerationManifest) -> bool) -> Vec<String> {
        self.datasets
            .iter()
            .filter(|d| select(d))
            .flat_map(|d| {
                std::iter::once(d.generation.clone())
                    .chain(d.objects.iter().map(|object| object.key.clone()))
            })
            .collect()
    }
    fn governance_path(&self, key: &str) -> PathBuf {
        PathBuf::from(
            self.declaration
                .root
                .to_string()
                .strip_prefix("file://")
                .unwrap(),
        )
        .join(key)
    }
    fn verify(&self, generation: &str) -> Result<String, String> {
        cli(
            &self.log(),
            &[
                "data",
                "verify",
                "--manifest",
                &format!(
                    "file://{}",
                    self.published().join(manifest_key(generation)).display()
                ),
                "--config",
                self.path.to_str().unwrap(),
            ],
        )
    }
}

fn import_pair(
    scratch: &Scratch,
    name: &str,
    role: DatasetRole,
    base: i64,
    rows: &[Row],
) -> Vec<GenerationManifest> {
    import_pair_text(
        scratch,
        name,
        role,
        [ticks(base, rows, 0), ticks(base, rows, 1)],
    )
}

fn import_pair_text(
    scratch: &Scratch,
    name: &str,
    role: DatasetRole,
    lines: [Vec<String>; 2],
) -> Vec<GenerationManifest> {
    fixture_config::import_ticks(
        &scratch.root,
        name,
        role,
        "pocket_option",
        &SYMBOLS,
        &SCALES,
        &lines,
    )
}

fn configure_bars(config: &mut Config) {
    use binary_alpha_engine::dataset::NativeGranularity;
    for instrument in &mut config.instruments {
        instrument.native_granularity = NativeGranularity::Bar { period_seconds: 5 };
        instrument.candles[0].min_observations = Some(4);
        instrument.candles[0].hard_min_observations = Some(4);
    }
    let research = config.research.as_mut().unwrap();
    let settlement = |contract: &mut binary_alpha_engine::execution::ContractTerms| {
        contract.settlement.max_settlement_delay_micros = 5_000_000;
        contract.settlement.max_tick_gap_micros = 5_000_000;
    };
    for instrument in &mut research.instruments {
        instrument.outcomes.expiry_seconds = vec![5, 7, 20];
        instrument.outcomes.max_entry_delay_ms = 5_000;
        instrument.outcomes.max_settlement_delay_ms = 5_000;
        instrument.outcomes.max_tick_gap_ms = 5_000;
        instrument.outcomes.true_jump_max_gap_ms = 5_000;
        instrument.search.embargo_micros = 10_000_000;
        for contract in &mut instrument.search.contracts {
            settlement(contract);
        }
    }
    research.portfolio.embargo_micros = 10_000_000;
    for binding in &mut research.portfolio.bindings {
        for alternative in &mut binding.alternatives {
            settlement(&mut alternative.contract);
        }
    }
    for scenario in &mut research.scenarios {
        for alternative in &mut scenario.alternatives {
            settlement(&mut alternative.contract);
        }
    }
}

fn import_bar_pair(
    scratch: &Scratch,
    name: &str,
    role: DatasetRole,
    base: i64,
    rows: &[Row],
) -> Vec<GenerationManifest> {
    let source = format!("sources/{name}");
    common::write_collection(
        &scratch.path(&source),
        &(0..2)
            .map(|i| common::AssetSpec {
                asset: SYMBOLS[i],
                expected_symbol_id: Some(i as i32 + 7),
                symbol_id: Some(i as i32 + 7),
                files: vec![bar_rows(base, rows, i)],
                metadata: true,
            })
            .collect::<Vec<_>>(),
    );
    let path = scratch.config(
        &format!("import-{name}.toml"),
        &scratch
            .bar_source()
            .replace("sources/bars", &source)
            .replace(
                "role = \"evaluation\"",
                &format!(
                    "role = \"{}\"",
                    if role == DatasetRole::Holdout {
                        DatasetRole::Evaluation
                    } else {
                        role
                    }
                ),
            ),
    );
    let lines = if role == DatasetRole::Holdout {
        let mut config = Config::parse(&fs::read_to_string(&path).unwrap()).unwrap();
        let binary_alpha_engine::config::Source::BarParquetCollection { role, .. } =
            &mut config.import.as_mut().unwrap().sources[0]
        else {
            unreachable!()
        };
        *role = DatasetRole::Holdout;
        common::current::import_config(&config, &scratch.root).unwrap()
    } else {
        common::current::import(&path).unwrap()
    };
    lines
        .iter()
        .map(|line| {
            GenerationManifest::from_json(
                &fs::read(
                    scratch
                        .path("published")
                        .join(manifest_key(&common::generation(line))),
                )
                .unwrap(),
            )
            .unwrap()
        })
        .collect()
}

/// Independent Phase 09 `joint` oracle: signal membership, ordered admission, exact native
/// postings and per-posting converted drawdown. Every contract finishes before the next row.
fn joint(
    rows: &[Row],
    deployments: &[(usize, bool, bool, bool)],
    worse: bool,
) -> (i64, i64, u64, [i64; 2]) {
    let (mut profit, mut peak, mut drawdown, mut settled, mut native) =
        (0_i64, 0_i64, 0_i64, 0_u64, [0_i64; 2]);
    for row in rows {
        for &(instrument, up, narrow, large) in deployments {
            if row.up != up || (narrow && row.wide) {
                continue;
            }
            let net = if row.win {
                (if large { 155 } else { 80 }) - if worse { 10 } else { 0 }
            } else if large {
                -205
            } else {
                -100
            };
            native[instrument] += net;
            profit += if instrument == 0 { net } else { net * 2 };
            peak = peak.max(profit);
            drawdown = drawdown.max(peak - profit);
            settled += 1;
        }
    }
    (profit, drawdown, settled, native)
}

#[test]
fn research_run_freezes_awaits_and_certifies() {
    assert_research_certifies(Fixture::new("phase11_complete"));
}

#[test]
fn five_stream_generated_search_publishes_and_verifies_in_research() {
    let mut fixture = Fixture::new("phase11_generated_five_streams");
    let streams: Vec<_> = [20, 40, 60, 80, 100]
        .into_iter()
        .map(|duration_seconds| StreamKey {
            duration_seconds,
            offset_seconds: 0,
        })
        .collect();
    for instrument in &mut fixture.config.instruments {
        let candle = instrument.candles[0].clone();
        instrument.candles = streams
            .iter()
            .map(|stream| binary_alpha_engine::config::CandleSpec {
                duration_seconds: stream.duration_seconds,
                offset_seconds: stream.offset_seconds,
                ..candle.clone()
            })
            .collect();
    }
    for instrument in &mut fixture.config.research.as_mut().unwrap().instruments {
        instrument.features.streams = Some(streams.clone());
        instrument.features.encodings = Some(Encodings {
            max_labels: 8,
            outputs: vec![EncodingSpec {
                output: "all_supported".into(),
                bins: None,
            }],
        });
        instrument.search.conditions = streams
            .iter()
            .map(|&stream| {
                SearchCondition::Generate(GeneratedSearchCondition {
                    stream,
                    output: "*".into(),
                    comparator: Comparator::Eq,
                })
            })
            .collect();
    }
    fixture.save();
    let report = fixture.run().unwrap();
    let (_, run) = fixture.run_record();
    assert_eq!(run.instruments.len(), 2, "{report}");
    for instrument in &run.instruments {
        let family = Family::from_json(&fixture.object(&instrument.family, "family.json")).unwrap();
        assert_eq!(family.schema_version, 2);
        assert_eq!(family.search.conditions.len(), 5);
        assert_eq!(family.resolved_conditions.as_ref().unwrap().len(), 8);
        assert!(family.lowering.is_none());
        assert_eq!(family.members.len(), 8);
        assert!(
            family
                .members
                .iter()
                .all(|member| member.global_index.is_some())
        );
        assert!(
            fixture
                .verify(&instrument.family)
                .unwrap()
                .contains("verified search generation")
        );
    }
}

fn reference_for_generation(published: &Path, generation: &str) -> String {
    format!(
        "file://{}",
        published.join(manifest_key(generation)).display()
    )
}

impl Fixture {
    /// Build the development evidence and a standalone search from the wide research settings.
    /// The caller may add a frozen evaluation input before running that search.
    fn wide_standalone_setup(
        &self,
        mut standalone: Config,
        path: &Path,
        published: &Path,
        development: &str,
    ) -> (Config, Value, String, String, String) {
        let uri = |generation: &str| {
            format!(
                "file://{}",
                published.join(manifest_key(generation)).display()
            )
        };
        write(path, standalone.canonical_toml());
        let audit = cli(
            &self.log(),
            &[
                "data",
                "audit",
                "--config",
                path.to_str().unwrap(),
                "--manifest",
                development,
            ],
        )
        .unwrap();
        let profile = common::generation(audit.lines().next().unwrap());
        cli(
            &self.log(),
            &["data", "verify", "--manifest", &uri(&profile)],
        )
        .unwrap();
        let mut feature =
            serde_json::to_value(&self.config.research.as_ref().unwrap().instruments[0].features)
                .unwrap();
        feature["role"] = "development".into();
        feature["input_manifest"] = development.into();
        feature["profile_manifest"] = uri(&profile).into();
        standalone.features =
            Some(serde_json::from_value(serde_json::json!({"instruments":[feature]})).unwrap());
        write(path, standalone.canonical_toml());
        let built = cli(
            &self.log(),
            &["features", "build", "--config", path.to_str().unwrap()],
        )
        .unwrap();
        let fit = common::generation(built.lines().next().unwrap());
        cli(&self.log(), &["data", "verify", "--manifest", &uri(&fit)]).unwrap();
        standalone.features = None;
        let mut outcomes =
            serde_json::to_value(&self.config.research.as_ref().unwrap().instruments[0].outcomes)
                .unwrap();
        outcomes["role"] = "development".into();
        outcomes["tick_manifest"] = development.into();
        outcomes["feature_manifest"] = uri(&fit).into();
        standalone.outcomes = Some(serde_json::from_value(outcomes).unwrap());
        write(path, standalone.canonical_toml());
        let built = cli(
            &self.log(),
            &["outcomes", "build", "--config", path.to_str().unwrap()],
        )
        .unwrap();
        let outcome = common::generation(built.lines().next().unwrap());
        cli(
            &self.log(),
            &["data", "verify", "--manifest", &uri(&outcome)],
        )
        .unwrap();
        standalone.outcomes = None;
        let mut search =
            serde_json::to_value(&self.config.research.as_ref().unwrap().instruments[0].search)
                .unwrap();
        let start = search
            .as_object_mut()
            .unwrap()
            .remove("decision_start")
            .unwrap();
        let end = search
            .as_object_mut()
            .unwrap()
            .remove("decision_end")
            .unwrap();
        search["development"] = serde_json::json!({
            "decision_start":start,"decision_end":end,
            "inputs":[{"tick_manifest":development,"feature_manifest":uri(&fit),"outcome_manifest":uri(&outcome)}]
        });
        standalone.search = Some(serde_json::from_value(search.clone()).unwrap());
        (standalone, search, profile, fit, outcome)
    }
}

#[test]
fn wide_research_daily_split_certifies_without_early_holdout_access() {
    let fixture = Fixture::wide_split("phase11_wide_daily_split");
    for dataset in fixture.datasets.iter().take(2) {
        command(&[
            "data",
            "verify",
            "--manifest",
            &uri(&fixture.scratch.root, &dataset.generation).to_string(),
        ])
        .unwrap();
    }
    for dataset in fixture.datasets.iter().skip(2) {
        if dataset.role != DatasetRole::Holdout {
            fixture.verify(&dataset.generation).unwrap();
        }
    }
    let development = fixture.config.research.as_ref().unwrap().instruments[0]
        .source_manifest
        .to_string();
    let mut standalone = binary_alpha_app::skeleton(&fixture.config);
    standalone.instruments = fixture.config.instruments.clone();
    let path = fixture.scratch.path("wide-standalone.toml");
    let (mut standalone, mut search, _, _, _) =
        fixture.wide_standalone_setup(standalone, &path, &fixture.published(), &development);
    write(&path, standalone.canonical_toml());
    let searched = cli(
        &fixture.log(),
        &["search", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    let searched_generation = common::generation(searched.lines().next().unwrap());
    fixture.verify(&searched_generation).unwrap();
    let standalone_family =
        Family::from_json(&fixture.object(&searched_generation, "family.json")).unwrap();
    assert_eq!(standalone_family.schema_version, 2);
    assert!(standalone_family.lowering.is_some());
    let stream = fixture.config.research.as_ref().unwrap().instruments[0]
        .search
        .conditions[0]
        .clone();
    let SearchCondition::Generate(rule) = stream else {
        unreachable!()
    };
    search["conditions"] = serde_json::json!([
        {"stream":rule.stream,"output":"candle_direction_auto_encoded","comparator":"eq","thresholds":["up"]},
        {"stream":rule.stream,"output":"candle_direction","comparator":"eq","thresholds":["up"]}
    ]);
    search["max_conditions"] = 1.into();
    search["scope"] = "exhaustive".into();
    search.as_object_mut().unwrap().remove("screen");
    standalone.search = Some(serde_json::from_value(search).unwrap());
    write(&path, standalone.canonical_toml());
    let parity = cli(
        &fixture.log(),
        &["search", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    let parity_generation = common::generation(parity.lines().next().unwrap());
    fixture.verify(&parity_generation).unwrap();
    let parity_family =
        Family::from_json(&fixture.object(&parity_generation, "family.json")).unwrap();
    let projected = parity_family
        .members
        .iter()
        .find(|member| member.conditions[0].output == "candle_direction_auto_encoded")
        .unwrap();
    let lowered = parity_family
        .members
        .iter()
        .find(|member| member.conditions[0].output == "candle_direction")
        .unwrap();
    assert_eq!(projected.raw, lowered.raw);
    assert_eq!(projected.development, lowered.development);
    let report = fixture.run().unwrap();
    let (manifest, run) = fixture.run_record();
    assert_eq!(
        run.state,
        RunState::AwaitingHoldoutAuthorization,
        "{report}"
    );
    assert_eq!(manifest.state, "awaiting_holdout_authorization");
    no_access(&logged(&fixture.log()), &fixture.protected());
    for (index, instrument) in run.instruments.iter().enumerate() {
        for generation in [
            &instrument.profile,
            &instrument.feature,
            &instrument.outcome,
            &instrument.family,
        ] {
            fixture.verify(generation).unwrap();
        }
        let plan =
            FeaturePlan::from_json(&fixture.object(&instrument.feature, "plan.json")).unwrap();
        let feature = FeatureManifest::from_json(
            &fs::read(fixture.published().join(manifest_key(&instrument.feature))).unwrap(),
        )
        .unwrap();
        let expected_streams = [(5, 0), (15, 5), (30, 15), (60, 30), (300, 150)];
        assert_eq!(plan.streams.len(), expected_streams.len());
        assert_eq!(feature.streams.len(), expected_streams.len());
        for (duration, offset) in expected_streams {
            let stream = plan
                .streams
                .iter()
                .find(|stream| {
                    stream.duration_seconds == duration && stream.offset_seconds == offset
                })
                .unwrap();
            let summary = feature
                .streams
                .iter()
                .find(|summary| {
                    summary.duration_seconds == duration && summary.offset_seconds == offset
                })
                .unwrap();
            let fit = plan
                .fit_windows
                .iter()
                .find(|fit| fit.duration_seconds == duration && fit.offset_seconds == offset)
                .unwrap();
            assert!(summary.rows >= 5, "unfilled {duration}s@{offset}s stream");
            assert_eq!(fit.rows, summary.rows);
            // §2a starts three rolling kinds at w=2, three at w=3, and three at w=4.
            // All eleven are predictive, so all_supported gives each an automatic encoding.
            for name in [
                "return_std_2_bps",
                "up_move_ratio_2",
                "range_position_2",
                "return_skew_3",
                "trend_r2_3",
                "trend_residual_3_bps",
                "return_kurtosis_4",
                "return_autocorr_4",
                "sign_reversal_rate_4",
                "range_overlap",
                "candle_pattern",
            ] {
                assert!(
                    stream.outputs.iter().any(|output| output.name == name),
                    "{duration}s@{offset}s missing {name}"
                );
                let encodings: Vec<_> = stream
                    .encodings
                    .iter()
                    .filter(|encoding| encoding.input == name)
                    .collect();
                assert_eq!(encodings.len(), 1, "{duration}s@{offset}s {name}");
                assert_eq!(encodings[0].output, format!("{name}_auto_encoded"));
                assert!(encodings[0].automatic);
                assert_eq!(
                    encodings[0].encoding,
                    if name == "candle_pattern" {
                        ProjectionKind::Category
                    } else {
                        ProjectionKind::DevelopmentFifths
                    }
                );
            }
        }
        let family = Family::from_json(&fixture.object(&instrument.family, "family.json")).unwrap();
        assert_eq!(family.schema_version, 2);
        assert!(
            family
                .resolved_conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|condition| condition.output.ends_with("_auto_encoded"))
        );
        assert!(family.lowering.is_some());
        assert!(family.members.iter().any(|member| member.rank.is_some()));
        assert!(family.members.iter().any(|member| {
            member.rank.is_some()
                && member
                    .conditions
                    .iter()
                    .any(|condition| condition.output == "candle_direction_auto_encoded")
                && member
                    .development
                    .as_ref()
                    .and_then(|group| group.profit.get(CURRENCIES[index]).copied().flatten())
                    .is_some_and(|profit| profit.compare(decimal("0")).unwrap().is_gt())
        }));
    }
    let selection = fixture.selection(&run);
    let settings = selection.config.portfolio.as_ref().unwrap();
    assert_eq!(settings.generate, Some(PortfolioGenerate { top: 8 }));
    assert!(
        settings
            .members
            .iter()
            .any(|member| !member.ordinals.is_empty())
    );
    for member in &settings.members {
        let record = &run.instruments[member.family];
        let plan = FeaturePlan::from_json(&fixture.object(&record.feature, "plan.json")).unwrap();
        let family = Family::from_json(&fixture.object(&record.family, "family.json")).unwrap();
        let source = family
            .members
            .iter()
            .find(|source| source.global_index == Some(member.member as u64))
            .unwrap();
        for ordinal in &member.ordinals {
            let condition = &source.conditions[ordinal.condition];
            let encoding = plan
                .stream(condition.stream)
                .unwrap()
                .encodings
                .iter()
                .find(|encoding| encoding.output == condition.output)
                .unwrap();
            assert_eq!(
                condition.threshold,
                Threshold::Text(encoding.interval_label(ordinal.ordinal).unwrap())
            );
        }
    }
    assert_eq!(
        fixture
            .config
            .research
            .as_ref()
            .unwrap()
            .qualification
            .gates
            .min_decisive,
        Some(1)
    );
    assert_eq!(
        fixture
            .config
            .research
            .as_ref()
            .unwrap()
            .qualification
            .gates
            .min_win_rate,
        Some(decimal("0.1"))
    );
    assert_eq!(run.outer.len(), 3);
    assert!(run.outer.iter().all(|result| {
        let projection = &result.outer.projection;
        let decisive = projection.wins.unwrap() + projection.losses.unwrap();
        projection.ties.is_some() && decisive >= 1 && projection.wins.unwrap() * 10 >= decisive
    }));
    fixture.verify(&run.selection).unwrap();
    fixture.verify(&manifest.generation).unwrap();
    let (_, grant) = fixture.grant();
    no_access(&logged(&fixture.log()), &fixture.protected());
    let certified = fixture.run().unwrap();
    assert!(certified.contains("verified research certification"));
    assert!(!certified.contains("envelope only"));
    let (certification, record) = fixture.certification(&grant);
    assert_eq!(certification.state, "certified");
    assert_eq!(record.verdict, Verdict::Pass);
    assert_eq!(record.holdout.len(), 2);
    assert_eq!(record.scenarios.len(), 3);
    for (scenario, expected) in
        record
            .scenarios
            .iter()
            .zip([(52, 30, 22, 0, 1), (51, 24, 27, 0, 2), (52, 30, 22, 0, 1)])
    {
        let projection = &scenario.outer.projection;
        assert_eq!(
            (
                projection.settled,
                projection.wins,
                projection.losses,
                projection.ties,
                projection.unresolved
            ),
            (
                expected.0,
                Some(expected.1),
                Some(expected.2),
                Some(expected.3),
                expected.4
            )
        );
        assert_eq!(scenario.verdict, Verdict::Pass);
    }
    fixture.verify(&certification.generation).unwrap();
}

#[cfg(feature = "cuda")]
fn wide_search_ready(fixture: &Fixture) -> Config {
    let development = &fixture.config.research.as_ref().unwrap().instruments[0].source_manifest;
    let mut standalone = binary_alpha_app::skeleton(&fixture.config);
    standalone.instruments = fixture.config.instruments.clone();
    let path = fixture.scratch.path("device-search-setup.toml");
    write(&path, standalone.canonical_toml());
    let audit = cli(
        &fixture.log(),
        &[
            "data",
            "audit",
            "--config",
            path.to_str().unwrap(),
            "--manifest",
            &development.to_string(),
        ],
    )
    .unwrap();
    let profile_generation = common::generation(audit.lines().next().unwrap());
    let profile = format!(
        "file://{}",
        fixture
            .published()
            .join(manifest_key(&profile_generation))
            .display()
    );
    let mut feature =
        serde_json::to_value(&fixture.config.research.as_ref().unwrap().instruments[0].features)
            .unwrap();
    feature["role"] = "development".into();
    feature["input_manifest"] = development.to_string().into();
    feature["profile_manifest"] = profile.into();
    standalone.features =
        Some(serde_json::from_value(serde_json::json!({"instruments":[feature]})).unwrap());
    write(&path, standalone.canonical_toml());
    let built = cli(
        &fixture.log(),
        &["features", "build", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    let feature_generation = common::generation(built.lines().next().unwrap());
    standalone.features = None;
    let mut outcomes =
        serde_json::to_value(&fixture.config.research.as_ref().unwrap().instruments[0].outcomes)
            .unwrap();
    outcomes["role"] = "development".into();
    outcomes["tick_manifest"] = development.to_string().into();
    outcomes["feature_manifest"] = format!(
        "file://{}",
        fixture
            .published()
            .join(manifest_key(&feature_generation))
            .display()
    )
    .into();
    standalone.outcomes = Some(serde_json::from_value(outcomes).unwrap());
    write(&path, standalone.canonical_toml());
    let built = cli(
        &fixture.log(),
        &["outcomes", "build", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    let outcome_generation = common::generation(built.lines().next().unwrap());
    standalone.outcomes = None;
    let mut search =
        serde_json::to_value(&fixture.config.research.as_ref().unwrap().instruments[0].search)
            .unwrap();
    let start = search
        .as_object_mut()
        .unwrap()
        .remove("decision_start")
        .unwrap();
    let end = search
        .as_object_mut()
        .unwrap()
        .remove("decision_end")
        .unwrap();
    search["development"] = serde_json::json!({
        "decision_start": start, "decision_end": end,
        "inputs":[{"tick_manifest": development,
            "feature_manifest": format!("file://{}", fixture.published().join(manifest_key(&feature_generation)).display()),
            "outcome_manifest": format!("file://{}", fixture.published().join(manifest_key(&outcome_generation)).display())}]
    });
    standalone.search = Some(serde_json::from_value(search).unwrap());
    standalone
}

/// The device job runs this on quantum for each change. Only process-local test knobs vary.
#[cfg(feature = "cuda")]
#[test]
fn wide_search_batches_blocks_and_devices_have_exact_parity() {
    let fixture = Fixture::wide_split("phase11_quantum_device_parity");
    let base = wide_search_ready(&fixture);
    let mut expected: Option<(Vec<u8>, Value, [u64; 6], u64)> = None;
    for (device, devices) in [
        ("cpu", vec![]),
        ("cuda", vec![0]),
        ("duplicate", vec![0, 0]),
    ] {
        for batch in [8, 16] {
            for threads in [32, 64] {
                let mut config = base.clone();
                if !devices.is_empty() {
                    config.accelerator = Some(
                        serde_json::from_value(serde_json::json!({
                            "backend":"cuda", "devices":devices
                        }))
                        .unwrap(),
                    );
                }
                let label = format!("{device}-{batch}-{threads}");
                let config_path = fixture.scratch.path(&format!("{label}.toml"));
                let digest_path = fixture.scratch.path(&format!("{label}-screen.json"));
                write(&config_path, config.canonical_toml());
                let result = Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
                    .args(["search", "--config", config_path.to_str().unwrap()])
                    .env("BINARY_ALPHA_SCREEN_BATCH_SIZE", batch.to_string())
                    .env("BINARY_ALPHA_SCREEN_MEMORY_BUDGET_BYTES", "45000")
                    .env("BINARY_ALPHA_SCREEN_THREADS_PER_BLOCK", threads.to_string())
                    .env("BINARY_ALPHA_TEST_SCREEN_DIGEST", &digest_path)
                    .output()
                    .unwrap();
                assert!(
                    result.status.success(),
                    "{label}: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
                let report = String::from_utf8(result.stdout).unwrap();
                let generation = common::generation(report.lines().next().unwrap());
                let family_bytes = fixture.object(&generation, "family.json");
                let digest: Value =
                    serde_json::from_slice(&fs::read(digest_path).unwrap()).unwrap();
                let first = report.lines().next().unwrap();
                let field_count = |name: &str| {
                    first
                        .split_whitespace()
                        .filter(|field| field.starts_with(name))
                        .count()
                };
                for name in [
                    "screen_batch",
                    "screen_budget",
                    "reservation_unit",
                    "local_hint_bytes",
                    "device=",
                    "sm_",
                    "build_target=",
                    "screen_threads=",
                ] {
                    assert_eq!(
                        field_count(name),
                        usize::from(device != "cpu"),
                        "{label}: {name}: {first}"
                    );
                }
                let counter = |name: &str| -> u64 {
                    let words: Vec<_> = first.split_whitespace().collect();
                    words.windows(2).find(|pair| pair[0] == name).unwrap()[1]
                        .parse()
                        .unwrap()
                };
                assert!(digest["members"].as_u64().unwrap() > batch);
                let counters = (batch == 8).then(|| {
                    assert!(counter("blocks") >= 3, "{label}: {report}");
                    (
                        [
                            "columns",
                            "blocks",
                            "tuples",
                            "list_entries",
                            "construction_visits",
                            "driver_visits",
                        ]
                        .map(counter),
                        counter("validation_visits"),
                    )
                });
                if let Some((bytes, reference, reference_counters, cpu_validation)) = &expected {
                    assert_eq!(&family_bytes, bytes, "family.json {label}");
                    assert_eq!(&digest, reference, "whole-family raw/BH/survivor {label}");
                    if let Some((counters, validation)) = counters {
                        assert_eq!(
                            &counters, reference_counters,
                            "non-transfer counters {label}"
                        );
                        assert_eq!(
                            validation,
                            if device == "duplicate" {
                                2 * cpu_validation
                            } else {
                                *cpu_validation
                            },
                            "workspace validations {label}"
                        );
                    }
                } else {
                    let (counters, validation) = counters.expect("first search is created");
                    assert!(validation > 0, "CPU workspace validations");
                    expected = Some((family_bytes, digest, counters, validation));
                }
                println!("{label}: {report}");
            }
        }
    }
}

#[cfg(feature = "cuda")]
#[test]
fn screening_overrides_leave_family_identity_and_bytes_unchanged() {
    let fixture = Fixture::wide_split("phase11_screening_override_identity");
    let config = wide_search_ready(&fixture);
    let path = fixture.scratch.path("screening-overrides.toml");
    let canonical = config.canonical_toml();
    let hash = config.content_hash();
    write(&path, canonical.clone());
    let run = |override_value: Option<(&str, &str)>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_binary-alpha"));
        command.args(["search", "--config", path.to_str().unwrap()]);
        if let Some((name, value)) = override_value {
            command.env(name, value);
        }
        command.output().unwrap()
    };
    let baseline = run(None);
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    let baseline_report = String::from_utf8(baseline.stdout).unwrap();
    let generation = common::generation(baseline_report.lines().next().unwrap());
    let family = fixture.object(&generation, "family.json");
    for (name, value) in [
        ("BINARY_ALPHA_SCREEN_THREADS_PER_BLOCK", "32"),
        ("BINARY_ALPHA_SCREEN_BATCH_SIZE", "8"),
        ("BINARY_ALPHA_SCREEN_MEMORY_BUDGET_BYTES", "45000"),
        ("BINARY_ALPHA_CUDA_RESERVATION_UNIT_BYTES", "64"),
    ] {
        let output = run(Some((name, value)));
        assert!(
            output.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report = String::from_utf8(output.stdout).unwrap();
        assert_eq!(
            common::generation(report.lines().next().unwrap()),
            generation,
            "{name}"
        );
        assert_eq!(fixture.object(&generation, "family.json"), family, "{name}");
        assert_eq!(config.content_hash(), hash, "{name}");
        assert_eq!(config.canonical_toml(), canonical, "{name}");
    }
    for name in [
        "BINARY_ALPHA_SCREEN_THREADS_PER_BLOCK",
        "BINARY_ALPHA_SCREEN_BATCH_SIZE",
        "BINARY_ALPHA_SCREEN_MEMORY_BUDGET_BYTES",
        "BINARY_ALPHA_CUDA_RESERVATION_UNIT_BYTES",
    ] {
        let output = run(Some((name, "0")));
        assert!(!output.status.success(), "{name} unexpectedly succeeded");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(name),
            "{name} was not named"
        );
    }
    let run_exact_budget = |budget: &str| {
        Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
            .args(["search", "--config", path.to_str().unwrap()])
            .env("BINARY_ALPHA_SCREEN_BATCH_SIZE", "1")
            .env("BINARY_ALPHA_SCREEN_MEMORY_BUDGET_BYTES", budget)
            .output()
            .unwrap()
    };
    let one_byte = run_exact_budget("1");
    assert!(!one_byte.status.success());
    let error = String::from_utf8_lossy(&one_byte.stderr);
    let required: usize = error
        .split("screen tuple budget exceeded: planned ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let below = (required - 1).to_string();
    let output = run_exact_budget(&below);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains(&format!(
        "planned {required} logical bytes, free budget {below}"
    )));
}

/// Manual release gate: the object contains only retained records while the manifest binds
/// the entire ten-million-member search population.
#[cfg(feature = "cuda")]
#[test]
#[ignore]
fn quantum_ten_million_member_schema_two_family() {
    use binary_alpha_engine::execution::Direction;
    let fixture = Fixture::wide_split("phase11_quantum_ten_million");
    let mut config = wide_search_ready(&fixture);
    let search = config.search.as_mut().unwrap();
    let stream = search.base_stream;
    search.min_conditions = 2;
    search.max_conditions = 2;
    search.max_candidates = 10_100_000;
    search.screen.as_mut().unwrap().top = Some(8);
    search.conditions = vec![SearchCondition::Named(
        binary_alpha_engine::config::NamedSearchCondition {
            stream,
            output: "range_bps".into(),
            comparator: Comparator::Gt,
            thresholds: (0..1119)
                .map(|index| Threshold::Number(1_000_000.0 + index as f64))
                .collect(),
        },
    )];
    let template = search.contracts[0].clone();
    search.contracts = (0..16)
        .map(|index| {
            let mut contract = template.clone();
            contract.id = format!("synthetic-{index}");
            contract.direction = if index % 2 == 0 {
                Direction::Buy
            } else {
                Direction::Sell
            };
            contract.win.gross_return = decimal(&format!("1.{:02}", 80 + index));
            contract
        })
        .collect();
    config.accelerator =
        Some(serde_json::from_value(serde_json::json!({"backend":"cuda","devices":[0]})).unwrap());
    let path = fixture.scratch.path("ten-million.toml");
    write(&path, config.canonical_toml());
    let started = Instant::now();
    let result = Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args(["search", "--config", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report = String::from_utf8(result.stdout).unwrap();
    assert!(
        report.lines().next().unwrap().contains(" blocks 1 "),
        "{report}"
    );
    let generation = common::generation(report.lines().next().unwrap());
    let manifest: binary_alpha_engine::search::FamilyManifest = serde_json::from_slice(
        &fs::read(fixture.published().join(manifest_key(&generation))).unwrap(),
    )
    .unwrap();
    assert!(manifest.members >= 10_000_000, "{report}");
    let bytes = fixture.object(&generation, "family.json");
    let family = Family::from_json(&bytes).unwrap();
    assert_eq!(family.schema_version, 2);
    assert!(
        bytes.len() < 10_000_000,
        "schema-2 object {} bytes",
        bytes.len()
    );
    let verified = command(&[
        "data",
        "verify",
        "--config",
        path.to_str().unwrap(),
        "--manifest",
        &format!(
            "file://{}",
            fixture
                .published()
                .join(manifest_key(&generation))
                .display()
        ),
    ])
    .unwrap();
    assert!(verified[0].starts_with("verified search generation "));
    println!(
        "ten-million report: {report} elapsed {:.3}s members {} family_bytes {} survivors {}",
        started.elapsed().as_secs_f64(),
        manifest.members,
        bytes.len(),
        family.members.len()
    );
}

#[test]
fn wide_research_without_holdout_evaluates_a_frozen_search_family() {
    let fixture = Fixture::wide_split("phase11_wide_without_holdout");
    let root = fixture.datasets.iter().take(2).collect::<Vec<_>>();
    let mut split = binary_alpha_app::skeleton(&fixture.config);
    split.storage.historical_data_dir =
        serde_json::from_value(serde_json::json!(fixture.scratch.path("free-retained"))).unwrap();
    split.storage.publication_uri = format!(
        "file://{}",
        fixture.scratch.path("free-published").display()
    )
    .parse()
    .unwrap();
    split.split = Some(DataSplit {
        namespace: "phase11-wide-without-holdout".into(),
        sources: root
            .iter()
            .map(|root| uri(&fixture.scratch.root, &root.generation))
            .collect(),
        development: vec![CoverageRange::new(BASE, BASE + DAY_MICROS)],
        evaluation: vec![CoverageRange::new(
            BASE + 3 * DAY_MICROS,
            BASE + 4 * DAY_MICROS,
        )],
        holdout: vec![],
    });
    let path = fixture.scratch.path("wide-free.toml");
    write(&path, split.canonical_toml());
    let report = cli_as(
        &fixture.log(),
        OPERATOR,
        &["data", "split", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    let declaration_uri = report
        .lines()
        .last()
        .unwrap()
        .strip_prefix("declaration ")
        .unwrap();
    let declaration = Declaration::from_json(
        &fs::read(declaration_uri.strip_prefix("file://").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(declaration.populations.len(), 4);
    assert!(
        declaration
            .populations
            .iter()
            .all(|population| population.role != DatasetRole::Holdout)
    );
    let published = fixture.scratch.path("free-published");
    for population in &declaration.populations {
        command(&[
            "data",
            "verify",
            "--manifest",
            &format!(
                "file://{}",
                published.join(manifest_key(&population.id)).display()
            ),
        ])
        .unwrap();
    }
    let reference = |index: usize| {
        format!(
            "file://{}",
            published
                .join(manifest_key(&declaration.populations[index].id))
                .display()
        )
    };
    let development = reference(0);
    let evaluation = reference(1);
    let mut standalone = binary_alpha_app::skeleton(&fixture.config);
    standalone.storage = split.storage;
    standalone.instruments = fixture.config.instruments.clone();
    let (mut standalone, mut search, profile_generation, fit_generation, _) =
        fixture.wide_standalone_setup(standalone, &path, &published, &development);
    let profile = reference_for_generation(&published, &profile_generation);
    let fit = reference_for_generation(&published, &fit_generation);
    let verify = |uri: &str| command(&["data", "verify", "--manifest", uri]).unwrap();
    standalone.features = Some(serde_json::from_value(serde_json::json!({"instruments":[{
        "role":"evaluation","input_manifest":evaluation,"profile_manifest":profile,"frozen_plan":fit
    }]})).unwrap());
    write(&path, standalone.canonical_toml());
    let applied = cli(
        &fixture.log(),
        &["features", "build", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    let applied = reference_for_generation(
        &published,
        &common::generation(applied.lines().next().unwrap()),
    );
    verify(&applied);
    standalone.features = None;
    search["evaluation"] = serde_json::json!({"decision_start":time(BASE + 3*DAY_MICROS),
        "decision_end":time(BASE + 3*DAY_MICROS + 81*CANDLE + 1_000_000),
        "inputs":[{"tick_manifest":evaluation,"feature_manifest":applied}]});
    standalone.search = Some(serde_json::from_value(search).unwrap());
    write(&path, standalone.canonical_toml());
    let searched = cli(
        &fixture.log(),
        &["search", "--config", path.to_str().unwrap()],
    )
    .unwrap();
    let family_uri = reference_for_generation(
        &published,
        &common::generation(searched.lines().next().unwrap()),
    );
    verify(&family_uri);
    let manifest = serde_json::from_slice::<Value>(
        &fs::read(family_uri.strip_prefix("file://").unwrap()).unwrap(),
    )
    .unwrap();
    let family_object = manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|object| object["path"] == "family.json")
        .unwrap();
    let family = Family::from_json(
        &fs::read(published.join(family_object["key"].as_str().unwrap())).unwrap(),
    )
    .unwrap();
    assert_eq!(family.schema_version, 2);
    assert!(family.search.evaluation.is_some());
    assert!(
        family
            .members
            .iter()
            .any(|member| member.rank.is_some() && member.evaluation.is_some())
    );
    assert!(
        family
            .members
            .iter()
            .filter(|member| member.rank.is_some())
            .all(|member| member.evaluation.is_some())
    );
}

fn tied_ticks(base: i64, rows: &[Row], instrument: usize) -> Vec<String> {
    let mut lines = ticks(base, rows, instrument);
    for candle in lines.chunks_mut(80) {
        let price = candle[0].rsplit_once(',').unwrap().1.to_string();
        for step in [20, 21] {
            let (time_and_symbol, _) = candle[step].rsplit_once(',').unwrap();
            candle[step] = format!("{time_and_symbol},{price}");
        }
    }
    lines
}

#[test]
fn decisive_research_gates_classify_synthetic_evaluation_and_holdout() {
    for (role, ties) in [
        (DatasetRole::Evaluation, true),
        (DatasetRole::Evaluation, false),
        (DatasetRole::Holdout, true),
        (DatasetRole::Holdout, false),
    ] {
        let name = format!("phase11_decisive_{role}_{ties}");
        let mut fixture = Fixture::at(
            Scratch::new(&name),
            if role == DatasetRole::Evaluation && !ties {
                LOSING
            } else {
                PLANTED
            },
            if role == DatasetRole::Holdout && !ties {
                LOSING
            } else {
                PLANTED
            },
        );
        if ties {
            let hour = if role == DatasetRole::Evaluation {
                3
            } else {
                4
            };
            let changed = import_pair_text(
                &fixture.scratch,
                "ties",
                role,
                [0, 1]
                    .map(|instrument| tied_ticks(BASE + hour * HOUR, &recipe(PLANTED), instrument)),
            );
            let offset = if role == DatasetRole::Evaluation {
                6
            } else {
                8
            };
            for (instrument, dataset) in changed.into_iter().enumerate() {
                let research = fixture.config.research.as_mut().unwrap();
                if role == DatasetRole::Evaluation {
                    research.evaluation.inputs[instrument] =
                        uri(&fixture.scratch.root, &dataset.generation);
                } else {
                    research.holdout.inputs[instrument] =
                        uri(&fixture.scratch.root, &dataset.generation);
                }
                fixture.declaration.populations[offset + instrument].generations =
                    vec![dataset.generation.clone()];
                fixture.datasets.push(dataset);
            }
        }
        let gates = &mut fixture
            .config
            .research
            .as_mut()
            .unwrap()
            .qualification
            .gates;
        gates.min_settled = 16;
        gates.min_decisive = Some(1);
        gates.min_win_rate = Some(decimal("0.6"));
        gates.min_profit = decimal("-1000");
        gates.max_drawdown = decimal("1000");
        fixture.save();
        fixture.run().unwrap();
        let (verdict, scenarios) = if role == DatasetRole::Evaluation {
            let (manifest, run) = fixture.run_record();
            let RunState::OuterRejected { verdict } = run.state else {
                panic!("expected outer rejection")
            };
            no_access(&logged(&fixture.log()), &fixture.protected());
            fixture.verify(&manifest.generation).unwrap();
            (verdict, run.outer)
        } else {
            let (_, run) = fixture.run_record();
            assert_eq!(run.state, RunState::AwaitingHoldoutAuthorization);
            no_access(&logged(&fixture.log()), &fixture.protected());
            let (_, grant) = fixture.grant();
            fixture.run().unwrap();
            let (manifest, record) = fixture.certification(&grant);
            assert_eq!(manifest.state, "rejected");
            fixture.verify(&manifest.generation).unwrap();
            (record.verdict, record.scenarios)
        };
        assert_eq!(
            verdict.reason(),
            Some(if ties {
                "insufficient_evidence"
            } else {
                "economic_failure"
            })
        );
        let projection = &scenarios[0].outer.projection;
        assert!(projection.settled >= 16);
        if ties {
            assert_eq!(projection.wins.unwrap() + projection.losses.unwrap(), 0);
            assert!(projection.ties.unwrap() >= 16);
            assert!(
                matches!(
                    verdict,
                    Verdict::InsufficientEvidence { ref reason }
                        if reason.contains("decisive trades 0")
                ),
                "{verdict:?}"
            );
        } else {
            assert!(projection.wins.unwrap() + projection.losses.unwrap() >= 1);
            assert!(
                projection
                    .failure
                    .as_ref()
                    .unwrap()
                    .contains("decisive win rate")
            );
        }
    }
}

fn generated_fixture(name: &str, top: u32) -> Fixture {
    let mut fixture = Fixture::new(name);
    let research = fixture.config.research.as_mut().unwrap();
    research.portfolio.generate = Some(PortfolioGenerate { top });
    research.portfolio.members.clear();
    research.portfolio.subsets.clear();
    for binding in &mut research.portfolio.bindings {
        binding.alternatives.truncate(1);
    }
    for instrument in &mut research.instruments {
        instrument.search.gates.min_net_profit = decimal("-1000");
        instrument.search.gates.max_unresolved = 100;
    }
    fixture.save();
    fixture
}

#[test]
fn generated_portfolio_selects_ranked_singletons_and_verifies_run() {
    let fixture = generated_fixture("phase11_generated_portfolio", 2);
    let report = fixture.run().unwrap();
    let (_, run) = fixture.run_record();
    let selection = fixture.selection(&run);
    let settings = selection.config.portfolio.as_ref().unwrap();
    assert_eq!(settings.generate, Some(PortfolioGenerate { top: 2 }));
    for research in [None, fixture.config.research.clone()] {
        let mut standalone = selection.config.clone();
        standalone.research = research;
        assert!(
            Config::parse(&standalone.canonical_toml())
                .unwrap_err()
                .to_string()
                .contains("portfolio.generate: only a research portfolio")
        );
    }
    assert!(settings.members.len() >= 2, "{report}");
    assert_eq!(settings.members.len(), settings.subsets.len());
    for (index, subset) in settings.subsets.iter().enumerate() {
        assert_eq!(subset.deployments.len(), 1);
        let deployment = subset.deployments[0];
        assert_eq!((deployment.member, deployment.repair), (index, 0));
        assert_eq!(
            settings.bindings[deployment.binding].instrument,
            INSTRUMENTS[settings.members[index].family]
        );
        let family = Family::from_json(&fixture.object(
            &run.instruments[settings.members[index].family].family,
            "family.json",
        ))
        .unwrap();
        let member = family
            .members
            .iter()
            .find(|member| member.global_index == Some(settings.members[index].member as u64))
            .unwrap();
        assert!(member.rank.is_some());
        assert_eq!(
            settings.bindings[deployment.binding].alternatives[0]
                .contract
                .id,
            member.contract
        );
    }
    assert!(
        fixture
            .verify(&run.selection)
            .unwrap()
            .contains("verified portfolio generation")
    );
    assert!(
        fixture
            .verify(&fixture.generation())
            .unwrap()
            .contains("verified research generation")
    );
    no_access(&logged(&fixture.log()), &fixture.protected());
}

#[test]
fn generated_top_syntax_fails_before_search_or_any_store_read() {
    let fixture = generated_fixture("phase11_generated_top_zero", 0);
    let error = fixture.run().unwrap_err();
    assert!(
        error.contains("portfolio.generate.top: must be positive"),
        "{error}"
    );
    assert!(logged(&fixture.log()).is_empty());
}

#[test]
fn generated_portfolio_validates_resolved_policy_count_before_folds() {
    let mut fixture = generated_fixture("phase11_generated_count", 2);
    fixture
        .config
        .research
        .as_mut()
        .unwrap()
        .portfolio
        .max_policies = 1;
    fixture.save();
    let error = fixture.run().unwrap_err();
    assert!(
        error.contains("portfolio.max_policies: the grid declares"),
        "{error}"
    );
    no_access(&logged(&fixture.log()), &fixture.evaluation());
    no_access(&logged(&fixture.log()), &fixture.protected());
}

#[test]
fn generated_portfolio_empty_selection_and_run_skip_outer_inputs() {
    let mut fixture = generated_fixture("phase11_generated_portfolio_empty", 2);
    for instrument in &mut fixture.config.research.as_mut().unwrap().instruments {
        instrument.search.gates.min_settled = 1_000_000;
    }
    fixture.save();
    let report = fixture.run().unwrap();
    let (_, run) = fixture.run_record();
    let selection = fixture.selection(&run);
    let settings = selection.config.portfolio.as_ref().unwrap();
    assert_eq!(selection.state, State::NoFeasiblePolicy, "{report}");
    assert_eq!(run.state, RunState::NoFeasiblePolicy);
    assert!(settings.members.is_empty() && settings.subsets.is_empty());
    assert_eq!(
        (
            selection.declared,
            selection.choices.len(),
            selection.selected
        ),
        (0, 0, None)
    );
    assert!(selection.refit.is_empty() && selection.outer.is_none());
    assert!(run.claims.is_empty() && run.outer.is_empty());
    fixture.verify(&run.selection).unwrap();
    fixture.verify(&fixture.generation()).unwrap();
    let log = logged(&fixture.log());
    no_access(&log, &fixture.evaluation());
    no_access(&log, &fixture.protected());
}

#[test]
fn no_feasible_run_rejects_fabricated_outer_claim() {
    let mut fixture = generated_fixture("phase11_empty_run_claim", 1);
    for instrument in &mut fixture.config.research.as_mut().unwrap().instruments {
        instrument.search.gates.min_settled = 1_000_000;
    }
    fixture.save();
    fixture.run().unwrap();
    let (mut manifest, mut run) = fixture.run_record();
    assert_eq!(run.state, RunState::NoFeasiblePolicy);
    run.claims.push("fabricated-assessment-claim".into());
    let bytes = run.to_json();
    let object = &mut manifest.objects[0];
    object.sha256 = research::digest(b"", &bytes);
    object.key = binary_alpha_engine::dataset::object_key(&object.sha256);
    object.bytes = bytes.len() as u64;
    object.crc32c = None;
    object.generation = None;
    write(&fixture.scratch.path("published").join(&object.key), bytes);
    let store =
        binary_alpha_app::store::Store::open(&fixture.config.storage.publication_uri).unwrap();
    let key = manifest.key();
    let error = binary_alpha_app::research::verify_run(
        &store.uri(&key),
        &store,
        &key,
        &manifest.to_json(),
        research::Access::ORDINARY,
    )
    .unwrap_err();
    assert!(error.contains("outer claims and results"), "{error}");
}

#[test]
fn generated_portfolio_rejects_wrong_instrument_and_multiple_alternatives() {
    for wrong in ["instrument", "alternative"] {
        let mut fixture = generated_fixture(&format!("phase11_generated_bad_{wrong}"), 1);
        let research = fixture.config.research.as_mut().unwrap();
        match wrong {
            "instrument" => research.portfolio.bindings[0].instrument = INSTRUMENTS[1].into(),
            "alternative" => {
                let original = fixture_config::configuration(&fixture.scratch.root);
                research.portfolio.bindings[0]
                    .alternatives
                    .push(original.research.unwrap().portfolio.bindings[0].alternatives[1].clone());
            }
            _ => unreachable!(),
        }
        fixture.save();
        let error = fixture.run().unwrap_err();
        assert!(
            error.contains("requires exactly one binding"),
            "{wrong}: {error}"
        );
        no_access(&logged(&fixture.log()), &fixture.protected());
    }
}

#[test]
fn generated_missing_binding_fails_before_fold_and_refit_profiles_publish() {
    let mut fixture = generated_fixture("phase11_missing_binding_profiles", 1);
    let research = fixture.config.research.as_mut().unwrap();
    for (input, fit) in research.folds[0]
        .inputs
        .iter_mut()
        .zip(&research.refit.fits)
    {
        input.fit_manifest = fit.clone();
    }
    let fit_generations: BTreeSet<_> = research
        .refit
        .fits
        .iter()
        .map(|fit| fit.generation().to_string())
        .collect();
    research.portfolio.bindings[0].instrument = INSTRUMENTS[1].into();
    fixture.save();
    let error = fixture.run().unwrap_err();
    assert!(error.contains("requires exactly one binding"), "{error}");
    for entry in fs::read_dir(fixture.scratch.path("published/manifests")).unwrap() {
        let path = entry.unwrap().path().join("ready.json");
        if !path.exists() {
            continue;
        }
        let manifest: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert!(
            manifest["kind"] != "instrument_stream"
                || !fit_generations.contains(manifest["source_generation"].as_str().unwrap()),
            "a fold-fit or refit profile was published before generated binding validation"
        );
    }
}

#[test]
fn generated_selection_verifier_rederives_every_member_and_subset() {
    let fixture = generated_fixture("phase11_generated_tamper", 1);
    fixture.run().unwrap();
    let (_, run) = fixture.run_record();
    let selection = fixture.selection(&run);
    let original = selection.config.portfolio.as_ref().unwrap();
    assert_eq!(original.members.len(), 2);
    let family =
        Family::from_json(&fixture.object(&run.instruments[0].family, "family.json")).unwrap();
    let lower = family
        .members
        .iter()
        .find(|member| member.rank == Some(2))
        .expect("second passing rank");
    let store =
        binary_alpha_app::store::Store::open(&fixture.config.storage.publication_uri).unwrap();
    let manifest = SelectionManifest::from_json(
        &fs::read(
            fixture
                .scratch
                .path("published")
                .join(manifest_key(&run.selection)),
        )
        .unwrap(),
    )
    .unwrap();
    for case in ["omitted", "reordered", "lower_rank", "ordinal", "binding"] {
        let mut changed = selection.clone();
        let settings = changed.config.portfolio.as_mut().unwrap();
        match case {
            "omitted" => {
                settings.members.pop();
                settings.subsets.pop();
            }
            "reordered" => {
                settings.members.swap(0, 1);
                settings.subsets.swap(0, 1);
            }
            "lower_rank" => settings.members[0].member = lower.global_index.unwrap() as usize,
            "ordinal" => settings.members[0]
                .ordinals
                .push(binary_alpha_engine::config::Ordinal {
                    condition: 0,
                    ordinal: 4,
                }),
            "binding" => settings.subsets[0].deployments[0].binding = 1,
            _ => unreachable!(),
        }
        let mut altered_manifest = manifest.clone();
        altered_manifest.config_hash = changed.config.content_hash();
        altered_manifest.generation = selection_generation_id(
            &altered_manifest.config_hash,
            &altered_manifest.code_revision,
            &altered_manifest.families,
        );
        let bytes = changed.to_json();
        let object = &mut altered_manifest.objects[0];
        object.sha256 = research::digest(b"", &bytes);
        object.key = binary_alpha_engine::dataset::object_key(&object.sha256);
        object.bytes = bytes.len() as u64;
        object.crc32c = None;
        object.generation = None;
        write(&fixture.scratch.path("published").join(&object.key), bytes);
        let key = altered_manifest.key();
        let uri = store.uri(&key);
        let error = binary_alpha_app::portfolio::verify_selection(
            &uri,
            &store,
            &key,
            &altered_manifest.to_json(),
            research::Access::ORDINARY,
        )
        .unwrap_err();
        assert!(
            error.contains("generated members and subsets differ"),
            "{case}: {error}"
        );
    }
    no_access(&logged(&fixture.log()), &fixture.protected());
}

#[test]
fn generated_ordinals_follow_fitted_edges_not_label_code_order() {
    use binary_alpha_engine::features::{FittedEncoding, ProjectionKind};
    let fixture = generated_fixture("phase11_generated_ordinals", 1);
    fixture.run().unwrap();
    let (_, run) = fixture.run_record();
    let selection = fixture.selection(&run);
    let settings = selection.config.portfolio.as_ref().unwrap();
    let mut families = Vec::new();
    let mut plans = Vec::new();
    for record in &run.instruments {
        families.push(Family::from_json(&fixture.object(&record.family, "family.json")).unwrap());
        plans.push(FeaturePlan::from_json(&fixture.object(&record.feature, "plan.json")).unwrap());
    }
    let ranked = families[0]
        .members
        .iter_mut()
        .find(|member| member.rank == Some(1))
        .unwrap();
    let source_index = ranked.global_index.unwrap() as usize;
    ranked.conditions[0].output = "edge_probe".into();
    ranked.conditions[0].threshold = Threshold::Text("3_to_4".into());
    plans[0].streams[0].encodings.push(FittedEncoding {
        output: "edge_probe".into(),
        input: "range_bps".into(),
        automatic: false,
        encoding: ProjectionKind::DevelopmentFifths,
        edges: Some(vec![1.0, 2.0, 3.0, 4.0]),
        input_divisor: 1.0,
        labels: vec![
            "4_to_inf".into(),
            "3_to_4".into(),
            "-inf_to_1".into(),
            "2_to_3".into(),
            "1_to_2".into(),
        ],
    });
    families[0].plan_identity = plans[0].identity();
    let (members, subsets) =
        binary_alpha_engine::portfolio::generated_members(settings, &families, &plans).unwrap();
    let generated = members
        .iter()
        .find(|member| member.family == 0 && member.member == source_index)
        .unwrap();
    assert_eq!(
        generated.ordinals,
        vec![binary_alpha_engine::config::Ordinal {
            condition: 0,
            ordinal: 3
        }]
    );
    assert_eq!(subsets.len(), members.len());
    let mut resolved = settings.clone();
    resolved.members = members;
    resolved.subsets = subsets;
    let logical = binary_alpha_engine::portfolio::logical_members(&resolved, &families).unwrap();
    let plans_by_instrument: BTreeMap<_, _> = plans
        .iter()
        .cloned()
        .map(|plan| (plan.instrument.clone(), plan))
        .collect();
    let policy = binary_alpha_engine::portfolio::policy(
        &resolved,
        &logical,
        &binary_alpha_engine::portfolio::ChoiceKey {
            subset: 0,
            alternatives: vec![0],
            risk_policy: 0,
        },
        binary_alpha_engine::portfolio::Form::Resolved(&plans_by_instrument),
    )
    .unwrap();
    assert_eq!(
        policy.strategies[0].conditions[0].threshold,
        Threshold::Text("3_to_4".into())
    );
    families[0]
        .members
        .iter_mut()
        .find(|member| member.rank == Some(1))
        .unwrap()
        .conditions[0]
        .threshold = Threshold::Text("not_an_interval".into());
    let (fallback, _) =
        binary_alpha_engine::portfolio::generated_members(settings, &families, &plans).unwrap();
    let second_rank = families[0]
        .members
        .iter()
        .find(|member| member.rank == Some(2))
        .unwrap();
    assert_eq!(
        fallback[0].member,
        second_rank.global_index.unwrap() as usize
    );
}

#[test]
fn generated_raw_output_precedes_same_named_fifths_through_folds() {
    use binary_alpha_engine::config::{Bins, NamedSearchCondition, Outputs};

    let mut fixture = generated_fixture("phase11_generated_raw_fifths_collision", 1);
    for instrument in &mut fixture.config.research.as_mut().unwrap().instruments {
        instrument.features.outputs = Some(Outputs::Named(vec!["body_bps".into()]));
        instrument.features.encodings = Some(Encodings {
            max_labels: 8,
            outputs: vec![EncodingSpec {
                output: "body_bps".into(),
                bins: Some(Bins::DevelopmentFifths),
            }],
        });
        instrument.search.conditions = vec![SearchCondition::Named(NamedSearchCondition {
            stream: instrument.search.base_stream,
            output: "body_bps".into(),
            comparator: Comparator::Ge,
            thresholds: vec![Threshold::Number(0.0)],
        })];
    }
    fixture.save();
    let report = fixture.run().unwrap();
    let (_, run) = fixture.run_record();
    let selection = fixture.selection(&run);
    let settings = selection.config.portfolio.as_ref().unwrap();
    assert!(!settings.members.is_empty(), "{report}");
    assert!(
        settings
            .members
            .iter()
            .all(|member| member.ordinals.is_empty())
    );
    assert!(selection.choices.iter().any(|choice| {
        choice
            .folds
            .iter()
            .any(|fold| fold.replay.is_some() && fold.inapplicable.is_none())
    }));
    fixture.verify(&run.selection).unwrap();
    fixture.verify(&fixture.generation()).unwrap();
}

#[test]
fn generated_fifths_ordinals_publish_and_verify_through_folds() {
    use binary_alpha_engine::config::Outputs;
    let mut fixture = generated_fixture("phase11_generated_fifths", 1);
    let varying = [0, 1].map(|instrument| {
        ticks(BASE, &recipe(PLANTED), instrument)
            .into_iter()
            .enumerate()
            .filter_map(|(position, line)| {
                let candle = position / 80;
                let step = position % 80;
                (matches!(step, 0 | 20 | 21 | 32 | 48 | 79) || step % (2 + candle % 5) == 0)
                    .then_some(line)
            })
            .collect::<Vec<_>>()
    });
    let sources = import_pair_text(
        &fixture.scratch,
        "fifths-source",
        DatasetRole::Development,
        varying,
    );
    for (index, source) in sources.into_iter().enumerate() {
        let reference = uri(&fixture.scratch.root, &source.generation);
        let research = fixture.config.research.as_mut().unwrap();
        research.instruments[index].source_manifest = reference.clone();
        research.folds[0].inputs[index].fit_manifest = reference;
        research.instruments[index].features.outputs =
            Some(Outputs::Named(vec!["tick_volume".into()]));
        research.instruments[index].features.encodings = Some(Encodings {
            max_labels: 8,
            outputs: vec![EncodingSpec {
                output: "tick_volume_dev_quantile".into(),
                bins: None,
            }],
        });
        research.instruments[index].search.conditions =
            vec![SearchCondition::Generate(GeneratedSearchCondition {
                stream: StreamKey {
                    duration_seconds: 20,
                    offset_seconds: 0,
                },
                output: "*".into(),
                comparator: Comparator::Eq,
            })];
        fixture.declaration.populations[index].coverage = source.coverage.clone();
        fixture.declaration.populations[index].source = "invented-variable-volume-v1".into();
        fixture.declaration.populations[index].generations = vec![source.generation.clone()];
        fixture.datasets[index] = source;
    }
    fixture.save();
    let report = fixture.run().unwrap();
    let (_, run) = fixture.run_record();
    let selection = fixture.selection(&run);
    let settings = selection.config.portfolio.as_ref().unwrap();
    assert!(!settings.members.is_empty(), "{report}");
    assert!(
        settings
            .members
            .iter()
            .all(|member| !member.ordinals.is_empty())
    );
    for member in &settings.members {
        let record = &run.instruments[member.family];
        let plan = FeaturePlan::from_json(&fixture.object(&record.feature, "plan.json")).unwrap();
        let family = Family::from_json(&fixture.object(&record.family, "family.json")).unwrap();
        let source = family
            .members
            .iter()
            .find(|source| source.global_index == Some(member.member as u64))
            .unwrap();
        for ordinal in &member.ordinals {
            let condition = &source.conditions[ordinal.condition];
            let encoding = plan
                .stream(condition.stream)
                .unwrap()
                .encodings
                .iter()
                .find(|encoding| encoding.output == condition.output)
                .unwrap();
            assert_eq!(
                condition.threshold,
                Threshold::Text(encoding.interval_label(ordinal.ordinal).unwrap())
            );
        }
    }
    assert!(
        selection
            .choices
            .iter()
            .all(|choice| choice.folds.iter().all(|fold| fold.inapplicable.is_none()))
    );
    assert!(
        selection
            .choices
            .iter()
            .any(|choice| { choice.folds.iter().any(|fold| fold.replay.is_some()) })
    );
    fixture.verify(&run.selection).unwrap();
    fixture.verify(&fixture.generation()).unwrap();
    no_access(&logged(&fixture.log()), &fixture.protected());
}

#[test]
fn bar_research_run_freezes_awaits_and_certifies() {
    assert_research_certifies(Fixture::bars("phase11_bars"));
}

#[test]
fn research_fit_cutoff_requires_observations_known_strictly_before_it() {
    use binary_alpha_engine::market::parse_event_time_micros;
    for bars in [false, true] {
        for refit in [false, true] {
            for offset in [-1, 0, 1] {
                let name = format!("phase11_cutoff_{bars}_{refit}_{offset}");
                let mut fixture = if bars {
                    Fixture::bars(&name)
                } else {
                    Fixture::new(&name)
                };
                let input = &fixture.datasets[if refit { 4 } else { 0 }];
                let known_at = parse_event_time_micros(&input.coverage.last_event_time).unwrap()
                    + if bars { 5_000_000 } else { 0 };
                let generation = input.generation.clone();
                let research = fixture.config.research.as_mut().unwrap();
                if refit {
                    research.refit.cutoff = time(known_at + offset);
                } else {
                    research.folds[0].cutoff = time(known_at + offset);
                }
                fixture.save();
                if offset <= 0 {
                    let error = fixture.run().unwrap_err();
                    let field = if refit {
                        "refit.fits[0]"
                    } else {
                        "folds[0].inputs[0].fit"
                    };
                    let expected = format!(
                        "{field}.input_manifest: the fitting coverage of generation {generation} ends at {}, not before the cutoff",
                        time(known_at)
                    );
                    assert_eq!(error.trim_end(), format!("research: {expected}"));
                    println!("cutoff bars {bars} refit {refit} offset {offset}: {error}");
                } else {
                    let report = fixture.run().unwrap();
                    assert_eq!(
                        fixture.run_record().1.state,
                        RunState::AwaitingHoldoutAuthorization,
                        "{report}"
                    );
                    assert!(report.contains("state awaiting_holdout_authorization"));
                    println!(
                        "cutoff bars {bars} refit {refit} offset {offset}: state awaiting_holdout_authorization"
                    );
                }
                no_access(&logged(&fixture.log()), &fixture.protected());
            }
        }
    }
}

fn assert_bar_outcomes(fixture: &Fixture, run: &Run) {
    use binary_alpha_engine::market::Bar;
    use binary_alpha_engine::outcomes::{
        MISSING_INDEX, OutcomeManifest, TICK_PRICE_OBJECT_PATH, TICK_TIME_OBJECT_PATH,
        stream_object_paths,
    };
    use binary_alpha_engine::stream::Observation;
    for (i, record) in run.instruments.iter().enumerate() {
        let manifest: OutcomeManifest =
            serde_json::from_value(fixture.manifest(&record.outcome)).unwrap();
        let plan = FeaturePlan::from_json(&fixture.object(&record.feature, "plan.json")).unwrap();
        assert_eq!(plan.price_scale.digits(), SCALES[i]);
        assert_eq!(manifest.tick_generation, fixture.datasets[i].generation);
        assert_eq!(manifest.tick_count, 130);
        let object_path = |path: &str| {
            let object = manifest.objects.iter().find(|o| o.path == path).unwrap();
            fixture.scratch.path("published").join(&object.key)
        };
        let read_i64 = |path: &str| common::read_le(&object_path(path), i64::from_le_bytes);
        let read_u32 = |path: &str| common::read_le(&object_path(path), u32::from_le_bytes);
        let expected = bar_rows(BASE, &recipe(PLANTED), i)
            .iter()
            .map(|row| {
                let [open, high, low, close, volume] = row.ohlcv;
                let bar = Bar {
                    start_unix_s: row.unix,
                    period_s: 5,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    provider: (),
                };
                let Observation::Bar(bar) = Observation::from_bar(&bar, plan.price_scale).unwrap()
                else {
                    unreachable!()
                };
                (bar.start_micros + bar.period_micros, bar.close)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            read_i64(TICK_TIME_OBJECT_PATH),
            expected.iter().map(|row| row.0).collect::<Vec<_>>()
        );
        assert_eq!(
            read_i64(TICK_PRICE_OBJECT_PATH),
            expected.iter().map(|row| row.1).collect::<Vec<_>>()
        );
        let paths = stream_object_paths(20, 0);
        let references = read_i64(&paths[0]);
        let entries = read_u32(&paths[1]);
        let settlements = read_u32(&paths[2]);
        let reasons = fixture.object(&record.outcome, &paths[3]);
        assert_eq!(
            references,
            (1..=32).map(|k| BASE + k * CANDLE).collect::<Vec<_>>()
        );
        assert_eq!(entries, (0..32).map(|k| 4 * k + 3).collect::<Vec<_>>());
        assert_eq!(&settlements[..3], &[4, 5, 7]);
        assert_eq!(&reasons[..3], &[0, 0, 0]);
        assert_eq!(&settlements[93..], &[128, 129, MISSING_INDEX]);
        assert_eq!(&reasons[93..], &[0, 0, 3]);
        println!(
            "bar outcome {} observations 130 rows 32 entry 3 settlements [4, 5, 7] tail [128, 129, missing] reasons [0, 0, 3]",
            INSTRUMENTS[i]
        );
    }
}

fn assert_bar_scenarios(fixture: &Fixture, run: &Run, certification: &CertificationRecord) {
    use binary_alpha_engine::dataset::{Layout, NativeGranularity};
    use binary_alpha_engine::execution::Outcome;
    let selection = fixture.selection(run);
    for (i, refit) in selection.refit.iter().enumerate() {
        assert_eq!(
            fixture.manifest(&refit.generation)["input_generation"],
            fixture.datasets[4 + i].generation
        );
        assert_eq!(
            fixture.manifest(&selection.folds[0].fits[i].generation)["input_generation"],
            fixture.datasets[i].generation
        );
    }
    for dataset in &fixture.datasets {
        assert_eq!(
            dataset.native_granularity,
            NativeGranularity::Bar { period_seconds: 5 }
        );
        assert_eq!(
            dataset.layout,
            (dataset.role == DatasetRole::Development).then_some(Layout::DailyV2)
        );
    }
    for (hour, results) in [(3, &run.outer), (4, &certification.scenarios)] {
        for result in results {
            for (i, feature) in result.outer.features.iter().enumerate() {
                assert_eq!(
                    fixture.manifest(&feature.generation)["input_generation"],
                    fixture.datasets[hour * 2 + i].generation
                );
            }
            let base = BASE + hour as i64 * HOUR;
            let delayed = result.scenario == "delayed";
            let delay = if delayed { 100_000 } else { 0 };
            let events = fixture.events(&result.outer.replay.generation);
            let signals = events
                .iter()
                .filter_map(|event| match &event.kind {
                    EventKind::Signal {
                        command: Some(command),
                        instrument,
                        close_time_micros,
                        ..
                    } => {
                        let i = INSTRUMENTS.iter().position(|id| id == instrument).unwrap();
                        let k = usize::try_from((close_time_micros - base) / CANDLE - 1).unwrap();
                        Some((command.as_str(), (i, k, *close_time_micros)))
                    }
                    _ => None,
                })
                .collect::<BTreeMap<_, _>>();
            assert_eq!(signals.len(), 16);
            let mut accepted = 0;
            let mut settled = 0;
            for event in &events {
                match &event.kind {
                    EventKind::Accepted {
                        command,
                        source,
                        entry_time_micros,
                        entry_price_units,
                        price_time_micros,
                        due_time_micros,
                        ..
                    } => {
                        let (i, _, close) = signals[command.as_str()];
                        let price = if i == 0 { 1_800_001 } else { 1_799_999 };
                        assert_eq!(
                            (*entry_time_micros, *price_time_micros, *due_time_micros),
                            (
                                Some(close + delay),
                                Some(close),
                                Some(close + delay + 5_000_000)
                            )
                        );
                        assert_eq!(*entry_price_units, Some(price));
                        assert_eq!(
                            (
                                source.provider_time_micros,
                                source.available_at_micros,
                                source.simulated
                            ),
                            (close, close + delay, true)
                        );
                        assert_eq!(event.time_micros, close + delay);
                        if accepted == 0 {
                            println!(
                                "bar acceptance hour {hour} {} price {} provider {} acceptance {} due {}",
                                result.scenario,
                                entry_price_units.unwrap(),
                                price_time_micros.unwrap(),
                                entry_time_micros.unwrap(),
                                due_time_micros.unwrap()
                            );
                        }
                        accepted += 1;
                    }
                    EventKind::Settled {
                        command,
                        source,
                        settlement_time_micros,
                        settlement_price_units,
                        outcome,
                        profit,
                        ..
                    } => {
                        let (i, k, close) = signals[command.as_str()];
                        let row = recipe(PLANTED)[k];
                        let entry = if i == 0 { 1_800_001 } else { 1_799_999 };
                        let price =
                            entry + (if row.win { 1 } else { -1 }) * if delayed { 3 } else { 2 };
                        let at = close + if delayed { 10_000_000 } else { 5_000_000 };
                        assert_eq!(
                            (*settlement_time_micros, *settlement_price_units),
                            (at, Some(price))
                        );
                        assert_eq!((source.provider_time_micros, event.time_micros), (at, at));
                        assert_eq!(*outcome, if row.win { Outcome::Win } else { Outcome::Loss });
                        assert_eq!(
                            *profit,
                            cents(if row.win {
                                if result.scenario == "worse_terms" {
                                    145
                                } else {
                                    155
                                }
                            } else {
                                -205
                            })
                        );
                        if settled == 0 {
                            println!(
                                "bar settlement hour {hour} {} price {} at {} outcome {outcome:?} profit {profit}",
                                result.scenario,
                                settlement_price_units.unwrap(),
                                settlement_time_micros
                            );
                        }
                        settled += 1;
                    }
                    _ => {}
                }
            }
            assert_eq!((accepted, settled), (16, 16));
            assert_eq!(result.outer.projection.settled, 16);
            println!(
                "bar scenario hour {hour} {} accepted {accepted} settled {settled} prices and acceptance clocks verified",
                result.scenario
            );
        }
    }
}

fn assert_research_certifies(fixture: Fixture) {
    let lines = fixture.run().unwrap();
    let (manifest, run) = fixture.run_record();
    assert_eq!(run.state, RunState::AwaitingHoldoutAuthorization, "{lines}");
    assert_eq!(manifest.state, "awaiting_holdout_authorization");
    let log = logged(&fixture.log());
    no_access(&log, &fixture.protected());
    assert!(!log.iter().any(|l| l.contains("holdout-use")));
    assert_development(&fixture, &manifest, &run, &lines);
    if fixture.datasets[0].native_granularity
        != binary_alpha_engine::dataset::NativeGranularity::Tick
    {
        println!("{lines}");
        assert_bar_outcomes(&fixture, &run);
    }
    assert_claims(&fixture, &run, ClaimKind::AssessmentUse, None);
    let first_read = log
        .iter()
        .position(|l| fixture.evaluation().iter().any(|g| l.contains(g)))
        .unwrap();
    let frozen_key = research::frozen_key(&manifest.generation);
    let freeze = log
        .iter()
        .position(|l| l == &format!("put_new {frozen_key}"))
        .unwrap();
    for key in &run.claims {
        let created = log
            .iter()
            .position(|l| l == &format!("put_new {key}"))
            .unwrap();
        assert!(freeze < created && created < first_read);
        assert_eq!(
            log[..first_read]
                .iter()
                .filter(|l| *l == &format!("read_to {key}"))
                .count(),
            2
        );
    }
    assert_scenarios(
        &fixture,
        &run.outer,
        &recipe(PLANTED),
        DatasetRole::Evaluation,
    );
    let expected = format!(
        "verified research generation {} state awaiting_holdout_authorization instruments 2 scenarios 3 objects 1 bytes {}",
        manifest.generation,
        fixture.object(&manifest.generation, "research.json").len()
    );
    assert!(lines.lines().any(|l| l == expected));
    assert_eq!(
        fixture.verify(&manifest.generation).unwrap(),
        format!("{expected}\n")
    );
    let (grant_line, grant) = fixture.grant();
    assert_eq!(grant.operator, OPERATOR);
    assert_eq!(
        grant_line,
        format!(
            "holdout grant {} research {} at file://{}\n",
            grant.hash,
            manifest.generation,
            fixture.grant_path().display()
        )
    );
    no_access(&logged(&fixture.log()), &fixture.protected());
    assert_eq!(grant.bundle_sha256, manifest.bundle_sha256());
    assert_eq!(grant.declaration, fixture.declaration.identity());
    assert_eq!(
        grant.tokens,
        ["holdout-0-a", "holdout-0-b", "holdout-1-a", "holdout-1-b"]
    );
    let (again, unchanged) = fixture.grant();
    assert_eq!(unchanged, grant);
    assert_eq!(
        again,
        format!(
            "holdout grant {} research {} at file://{} (already created {})\n",
            grant.hash,
            manifest.generation,
            fixture.grant_path().display(),
            grant.created_at
        )
    );
    let report = fixture.run().unwrap();
    let certification_log = logged(&fixture.log());
    let (certification, record) = fixture.certification(&grant);
    if fixture.datasets[0].native_granularity
        != binary_alpha_engine::dataset::NativeGranularity::Tick
    {
        println!("{grant_line}{report}");
        assert_bar_scenarios(&fixture, &run, &record);
        let expected = format!(
            "verified research certification {} state certified scenarios 3 objects 1 bytes {}",
            certification.generation,
            fixture
                .object(&certification.generation, "certification.json")
                .len()
        );
        assert!(report.lines().any(|line| line == expected));
    }
    assert_eq!(certification.state, "certified", "{report}");
    assert_eq!(record.verdict, Verdict::Pass);
    assert_eq!(certification.grant, grant.hash);
    assert_eq!(record.grant, grant.hash);
    assert_eq!(record.frozen, run.frozen.as_ref().unwrap().as_str());
    assert_eq!(record.holdout, grant.holdout);
    assert_eq!(record.bundle_sha256, manifest.bundle_sha256());
    assert_claims(&fixture, &run, ClaimKind::HoldoutUse, Some(&grant));
    let receipt_key = fixture.declaration.key(&research::receipt_key(&grant.hash));
    let receipt =
        Receipt::from_json(&fs::read(fixture.governance_path(&receipt_key)).unwrap()).unwrap();
    assert_eq!(
        receipt,
        Receipt {
            schema_version: 1,
            grant: grant.hash.clone(),
            research: manifest.generation.clone(),
            bundle_sha256: grant.bundle_sha256.clone(),
            holdout: grant.holdout.clone(),
            claims: record.claims.clone(),
            declaration: fixture.declaration.identity()
        }
    );
    let receipt_at = certification_log
        .iter()
        .position(|l| l == &format!("put_new {receipt_key}"))
        .unwrap();
    let protected = fixture
        .protected()
        .into_iter()
        .chain(
            record.scenarios[0]
                .outer
                .features
                .iter()
                .map(|f| f.generation.clone()),
        )
        .collect::<Vec<_>>();
    no_access(&certification_log[..receipt_at], &protected);
    // Population claims MUST precede the receipt, which references the complete matching set.
    // The receipt is the boundary for protected data, not for the non-sensitive claim keys.
    let claim_puts = certification_log[..receipt_at]
        .iter()
        .filter(|l| l.starts_with("put_new phase11/holdout-use/"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        claim_puts,
        record
            .claims
            .iter()
            .map(|k| format!("put_new {k}"))
            .collect::<Vec<_>>()
    );
    assert_scenarios(
        &fixture,
        &record.scenarios,
        &recipe(PLANTED),
        DatasetRole::Holdout,
    );
    assert_eq!(
        fixture.verify(&certification.generation).unwrap(),
        format!(
            "verified research certification {} state certified envelope only: protected evidence is verified within the authorized certification run\n",
            certification.generation
        )
    );
    let public_log = logged(&fixture.log());
    no_access(&public_log, &protected);
    let key = fixture.manifest(&certification.generation)["objects"][0]["key"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(!public_log.iter().any(|l| l.contains(&key)));
    for generation in [
        &record.scenarios[0].outer.replay.generation,
        &record.scenarios[0].outer.features[0].generation,
    ] {
        assert!(fixture.verify(generation).unwrap_err().contains(PROTECTED));
        // The public envelope is read; no object of it and no holdout dataset key is touched.
        let log = logged(&fixture.log());
        no_access(&log, &fixture.protected());
        let objects: Vec<String> = fixture.manifest(generation)["objects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|object| object["key"].as_str().unwrap().to_owned())
            .collect();
        assert!(!objects.is_empty());
        no_access(&log, &objects);
    }
    let before = manifest_snapshot(&fixture.scratch.path("published"));
    assert!(fixture.run().unwrap().contains("(already published)"));
    assert_eq!(fixture.certification(&grant), (certification, record));
    assert_manifest_snapshot(&fixture.scratch.path("published"), &before);
}

#[test]
fn verify_run_refuses_changed_account_capital_currency_or_scale() {
    let fixture = Fixture::new("phase12_account_verification");
    fixture.run().unwrap();
    let (manifest, run) = fixture.run_record();
    let store =
        binary_alpha_app::store::Store::open(&fixture.config.storage.publication_uri).unwrap();
    let before = manifest_snapshot(&fixture.scratch.path("published"));
    for field in ["capital", "currency", "scale"] {
        let mut changed = run.clone();
        let account = &mut changed.config.research.as_mut().unwrap().portfolio.accounts[0];
        match field {
            "capital" => {
                account.initial_cash = account.initial_cash.checked_add(decimal("1")).unwrap()
            }
            "currency" => account.currency = "other".to_string().try_into().unwrap(),
            "scale" => account.scale += 1,
            _ => unreachable!(),
        }
        let mut changed_manifest = manifest.clone();
        changed_manifest.config_hash = changed.config.content_hash();
        changed_manifest.generation = research::run_generation_id(
            &changed_manifest.config_hash,
            &changed_manifest.code_revision,
            &changed_manifest.declaration,
        );
        let bytes = changed.to_json();
        let object = &mut changed_manifest.objects[0];
        object.sha256 = research::digest(b"", &bytes);
        object.key = binary_alpha_engine::dataset::object_key(&object.sha256);
        object.bytes = bytes.len() as u64;
        write(&fixture.scratch.path("published").join(&object.key), bytes);
        let key = changed_manifest.key();
        let uri = store.uri(&key);
        let error = binary_alpha_app::research::verify_run(
            &uri,
            &store,
            &key,
            &changed_manifest.to_json(),
            research::Access::ORDINARY,
        )
        .unwrap_err();
        assert_eq!(
            error,
            format!(
                "{uri}: the run record does not carry the manifest's configuration, declaration, selection, state, and descriptor"
            ),
            "{field}"
        );
    }
    assert_manifest_snapshot(&fixture.scratch.path("published"), &before);
    no_access(&logged(&fixture.log()), &fixture.protected());
}

fn assert_development(fixture: &Fixture, manifest: &RunManifest, run: &Run, report: &str) {
    assert_eq!(run.instruments.len(), 2);
    let research_config = fixture.config.research.as_ref().unwrap();
    for (i, record) in run.instruments.iter().enumerate() {
        assert_eq!(record.instrument, INSTRUMENTS[i]);
        assert_eq!(
            record.source,
            research_config.instruments[i].source_manifest.generation()
        );
        for (generation, kind) in [
            (&record.profile, "instrument_stream"),
            (&record.feature, "feature_generation"),
            (&record.outcome, "outcome_generation"),
            (&record.family, "search_family"),
        ] {
            assert_eq!(fixture.manifest(generation)["kind"], kind);
            assert!(report.contains(generation));
        }
        let family = Family::from_json(&fixture.object(&record.family, "family.json")).unwrap();
        assert_eq!(family.members.len(), 2);
        assert_eq!(family.search.evaluation, None);
        assert_eq!(
            family.search.scope,
            research_config.instruments[i].search.scope
        );
        for (member, label, profit) in [(0, "up", -160), (1, "down", 20)] {
            assert_eq!(
                family.members[member].conditions[0].threshold,
                Threshold::Text(label.into())
            );
            assert_eq!(
                family.members[member].development.as_ref().unwrap().profit[CURRENCIES[i]],
                Some(cents(profit))
            );
            assert_eq!(family.members[member].screened, None);
            assert_eq!(family.members[member].evaluation, None);
        }
        let feature = FeatureManifest::from_json(
            &fs::read(
                fixture
                    .scratch
                    .path("published")
                    .join(manifest_key(&record.feature)),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(feature.input_generation, record.source);
        assert_eq!(feature.profile_generation, record.profile);
        assert_eq!(feature.frozen_from, None);
    }
    let selection = fixture.selection(run);
    assert_eq!(selection.state, State::Selected);
    assert_eq!(
        (selection.declared, selection.rejected, selection.valid),
        (12, 0, 12)
    );
    assert_eq!(
        selection.config.portfolio.as_ref().unwrap().evaluation,
        None
    );
    assert_eq!(selection.outer, None);
    assert_eq!(selection.folds.len(), 1);
    assert_eq!(selection.folds[0].fits.len(), 2);
    assert_eq!(selection.refit.len(), 2);
    assert_eq!(
        selection
            .families
            .iter()
            .map(|f| f.generation.clone())
            .collect::<Vec<_>>(),
        run.instruments
            .iter()
            .map(|i| i.family.clone())
            .collect::<Vec<_>>()
    );
    let logical = [
        vec![(0, true, true)],
        vec![(1, false, true)],
        vec![(0, true, true), (1, false, true)],
        vec![(0, true, false)],
        vec![(1, false, false)],
    ];
    let mut passing = 0;
    for choice in &selection.choices {
        let deployments = logical[choice.subset]
            .iter()
            .zip(&choice.alternatives)
            .map(|(&(i, up, narrow), &alt)| (i, up, narrow, alt == 1))
            .collect::<Vec<_>>();
        let (profit, drawdown, settled, _) = joint(&recipe(PLANTED), &deployments, false);
        let projection = choice.folds[0].projection.as_ref().unwrap();
        assert_eq!(
            (projection.profit, projection.drawdown, projection.settled),
            (Some(cents(profit)), Some(cents(drawdown)), settled),
            "{choice:?}"
        );
        assert_eq!(choice.rank.is_some(), profit >= 0);
        passing += u64::from(profit >= 0);
    }
    assert_eq!(selection.passing, passing);
    let winner = &selection.choices[selection.selected.unwrap()];
    assert_eq!(
        (
            winner.subset,
            winner.alternatives.clone(),
            winner.risk_policy
        ),
        (2, vec![1, 1], 0)
    );
    assert_eq!(winner.profit, Some(decimal("8.40")));
    let policy = selection.frozen.as_ref().unwrap();
    assert_eq!(policy.bindings.len(), 2);
    for (i, (binding, strategy)) in policy.bindings.iter().zip(&policy.strategies).enumerate() {
        assert_eq!(binding.instrument, INSTRUMENTS[i]);
        assert_eq!(binding.account, format!("a{i}"));
        assert_eq!(binding.contract, format!("{i}-large"));
        assert_eq!(strategy.plan_identity, selection.refit[i].plan_identity);
        assert_eq!(
            strategy.conditions[0].threshold,
            Threshold::Text(if i == 0 { "up" } else { "down" }.into())
        );
        let plan =
            FeaturePlan::from_json(&fixture.object(&selection.refit[i].generation, "plan.json"))
                .unwrap();
        assert_eq!(plan.identity(), strategy.plan_identity);
    }
    let bytes = fs::read(
        fixture
            .scratch
            .path("published")
            .join(research::frozen_key(&manifest.generation)),
    )
    .unwrap();
    let frozen = Frozen::from_json(&bytes).unwrap();
    assert_eq!(run.frozen, Some(research::digest(b"", &bytes)));
    assert_eq!(
        frozen,
        Frozen {
            research: manifest.generation.clone(),
            intent: run.intent.clone(),
            declaration: fixture.declaration.identity(),
            instruments: run.instruments.clone(),
            selection: run.selection.clone(),
            scenarios: research_config.scenarios.clone(),
            descriptor: run.descriptor.clone()
        }
    );
    assert_eq!(run.descriptor, research::descriptor(research_config));
    assert_eq!(run.descriptor.claim, "empirical_policy_qualification_v1");
    assert_eq!(run.descriptor.look, "fixed_horizon_once");
    assert_eq!(run.descriptor.benchmark, "analytic_zero_profit");
    assert_eq!(run.descriptor.market_inference, "unavailable");
    assert_eq!(run.descriptor.horizon, "last_input_tick_per_instrument");
    assert_eq!(run.descriptor.accounts, research_config.portfolio.accounts);
    run.complete_bundle().unwrap();
    let intent =
        Intent::from_json(&fs::read(fixture.governance_path(&run.intent)).unwrap()).unwrap();
    assert_eq!(intent.config_hash, fixture.config.content_hash());
    assert_eq!(intent.declaration, fixture.declaration.identity());
    assert_eq!(intent.populations.len(), 10);
    assert_eq!(manifest.generation, fixture.generation());
    assert_eq!(
        manifest.bundle_sha256(),
        research::digest(b"", &run.to_json())
    );
}

fn assert_claims(fixture: &Fixture, run: &Run, kind: ClaimKind, grant: Option<&Grant>) {
    let inputs = if kind == ClaimKind::AssessmentUse {
        &fixture.config.research.as_ref().unwrap().evaluation.inputs
    } else {
        &fixture.config.research.as_ref().unwrap().holdout.inputs
    };
    let tokens = fixture
        .declaration
        .tokens(inputs.iter().map(ManifestUri::generation))
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    if kind == ClaimKind::AssessmentUse {
        assert_eq!(
            run.claims,
            tokens
                .iter()
                .map(|token| fixture.declaration.key(&research::claim_key(kind, token)))
                .collect::<Vec<_>>()
        );
    }
    for token in &tokens {
        let expected = Claim {
            schema_version: 1,
            kind,
            token: token.clone(),
            study: "synthetic".into(),
            attempt: "first".into(),
            research: fixture.generation(),
            frozen: run.frozen.clone().unwrap(),
            declaration: fixture.declaration.identity(),
            tokens: tokens.clone(),
            grant: grant.map(|g| g.hash.clone()),
        };
        let key = fixture.declaration.key(&research::claim_key(kind, token));
        assert_eq!(
            fs::read(fixture.governance_path(&key)).unwrap(),
            research::to_json(&expected)
        );
    }
}

fn assert_scenarios(
    fixture: &Fixture,
    results: &[research::ScenarioResult],
    rows: &[Row],
    role: DatasetRole,
) {
    assert_eq!(
        results
            .iter()
            .map(|r| r.scenario.as_str())
            .collect::<Vec<_>>(),
        ["baseline", "delayed", "worse_terms"]
    );
    for result in results {
        let (profit, drawdown, settled, native) = joint(
            rows,
            &[(0, true, true, true), (1, false, true, true)],
            result.scenario == "worse_terms",
        );
        let projection = &result.outer.projection;
        assert_eq!(
            (
                projection.profit,
                projection.drawdown,
                projection.settled,
                projection.unresolved
            ),
            (Some(cents(profit)), Some(cents(drawdown)), settled, 0),
            "{}",
            result.scenario
        );
        assert_eq!(projection.rates, BTreeSet::from(["eur-unit".into()]));
        assert_eq!(projection.unavailable_observations, 0);
        assert_eq!(projection.failure, None);
        assert_eq!(result.verdict, Verdict::Pass);
        let summary = fixture.summary(&result.outer.replay.generation);
        assert_eq!(summary.reporting.currency, "unit");
        assert_eq!(summary.reporting.scale, 2);
        assert_eq!(
            summary.reporting.settled_equity,
            Some(cents(3_000_000 + profit))
        );
        assert_eq!(summary.reporting.max_drawdown, Some(cents(drawdown)));
        assert_eq!(projection.valued_at, summary.last_time_micros.map(time));
        assert_eq!(
            summary.last_time_micros,
            Some(
                BASE + if role == DatasetRole::Holdout {
                    4 * HOUR
                } else {
                    3 * HOUR
                } + 32 * CANDLE
            )
        );
        for (i, account) in summary.accounts.iter().enumerate() {
            assert_eq!(account.completed_profit, cents(native[i]));
            assert_eq!(account.cash, cents(1_000_000 + native[i]));
            assert_eq!(account.open, 0);
            assert!(account.reserved.is_zero() && account.paid_basis.is_zero());
        }
        assert_eq!(summary.portfolio.settled, settled);
        assert_eq!(summary.portfolio.unresolved, 0);
        assert_eq!(summary.splits, result.outer.splits);
        assert_eq!(
            summary.splits["a"].settled + summary.splits["b"].settled,
            settled
        );
        assert_eq!(
            fixture.manifest(&result.outer.replay.generation)["role"],
            role.to_string()
        );
        let (_, run) = fixture.run_record();
        let selection = fixture.selection(&run);
        for (i, feature) in result.outer.features.iter().enumerate() {
            let manifest = fixture.manifest(&feature.generation);
            assert_eq!(manifest["role"], role.to_string());
            assert_eq!(manifest["frozen_from"], selection.refit[i].generation);
            assert_eq!(feature.plan_identity, selection.refit[i].plan_identity);
        }
    }
}

fn manifest_snapshot(root: &Path) -> BTreeMap<PathBuf, (Vec<u8>, SystemTime)> {
    // The publisher writes the destination manifest before the retained copy, so a process
    // killed at the first boundary can leave no retained manifests directory yet.
    let entries = match fs::read_dir(root.join("manifests")) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return BTreeMap::new(),
        Err(error) => panic!("{}: {error}", root.display()),
    };
    entries
        .map(|e| e.unwrap().path().join("ready.json"))
        .filter(|p| p.is_file())
        .map(|path| {
            (
                path.strip_prefix(root).unwrap().to_owned(),
                (
                    fs::read(&path).unwrap(),
                    fs::metadata(&path).unwrap().modified().unwrap(),
                ),
            )
        })
        .collect()
}
fn assert_manifest_snapshot(root: &Path, before: &BTreeMap<PathBuf, (Vec<u8>, SystemTime)>) {
    for (key, (bytes, modified)) in before {
        let path = root.join(key);
        assert_eq!(fs::read(&path).unwrap(), *bytes);
        assert_eq!(
            fs::metadata(path).unwrap().modified().unwrap(),
            *modified,
            "{} was rewritten",
            key.display()
        );
    }
}

#[test]
#[ignore = "requires an explicitly supplied governed development/evaluation configuration"]
fn governed_research() {
    let Ok(path) = std::env::var("BINARY_ALPHA_TEST_CONFIG") else {
        eprintln!("governed_research unavailable: BINARY_ALPHA_TEST_CONFIG is unset");
        return;
    };
    // Issue #12's governed proof is development/evaluation only. Refuse a pre-existing
    // grant before invoking the same command that could otherwise resume certification.
    let config = binary_alpha_app::load_config(Path::new(&path)).unwrap();
    let declaration = binary_alpha_app::research::declaration(&config)
        .unwrap()
        .expect("governed_research requires research.study");
    let generation = research::run_generation_id(
        &config.content_hash(),
        binary_alpha_app::import::CODE_REVISION,
        &declaration.identity(),
    );
    let governance = binary_alpha_app::store::Store::open(&declaration.root).unwrap();
    assert!(
        governance
            .head(&format!(
                "{}/{}",
                declaration.namespace,
                research::grant_key(&generation)
            ))
            .unwrap()
            .is_none(),
        "governed_research requires an awaiting-only configuration without a holdout grant"
    );
    let result = common::binary_alpha(&["research", "run", "--config", &path]);
    let report = output(result).unwrap();
    println!("{report}");
    assert!(report.contains("state awaiting_holdout_authorization"));
}

fn small_replay(name: &str) -> Fixture {
    let scratch = Scratch::new(name);
    let rows = vec![
        Row {
            up: false,
            wide: false,
            win: true
        };
        3
    ];
    let mut a = ticks(BASE, &rows[..2], 0);
    a.retain(|line| {
        binary_alpha_engine::market::parse_event_time_micros(line.split(',').next().unwrap())
            .unwrap()
            <= BASE + 41_000_000
    });
    let datasets = import_pair_text(
        &scratch,
        "replay",
        DatasetRole::Development,
        [a, ticks(BASE, &rows, 1)],
    );
    let mut config = fixture_config::replay_configuration(&scratch.root);
    config.replay.as_mut().unwrap().decision_end = time(BASE + 80_000_000);
    let audit_config = scratch.path("audit.toml");
    let mut audit = Config {
        replay: None,
        ..config.clone()
    };
    write(&audit_config, audit.canonical_toml());
    for (i, dataset) in datasets.iter().enumerate() {
        let tick = uri(&scratch.root, &dataset.generation);
        let report = command(&[
            "data",
            "audit",
            "--config",
            audit_config.to_str().unwrap(),
            "--manifest",
            &tick.to_string(),
        ])
        .unwrap();
        let profile = uri(&scratch.root, &common::generation(&report[0]));
        audit.features=Some(serde_json::from_value(serde_json::json!({"instruments":[{
            "role":"development","input_manifest":tick,"profile_manifest":profile,
            "streams":[{"duration_seconds":20,"offset_seconds":0}],"outputs":["candle_direction","range_bps"]}]})).unwrap());
        let feature_config = scratch.path(&format!("features-{i}.toml"));
        write(&feature_config, audit.canonical_toml());
        let report = command(&[
            "features",
            "build",
            "--config",
            feature_config.to_str().unwrap(),
        ])
        .unwrap();
        let feature = common::generation(&report[0]);
        let manifest = FeatureManifest::from_json(
            &fs::read(scratch.path("published").join(manifest_key(&feature))).unwrap(),
        )
        .unwrap();
        let plan_key = &manifest
            .objects
            .iter()
            .find(|o| o.path == "plan.json")
            .unwrap()
            .key;
        let plan =
            FeaturePlan::from_json(&fs::read(scratch.path("published").join(plan_key)).unwrap())
                .unwrap();
        let replay = config.replay.as_mut().unwrap();
        replay.inputs[i].tick_manifest = tick;
        replay.inputs[i].feature_manifest = uri(&scratch.root, &feature);
        replay.strategies[i].plan_identity = plan.identity();
    }
    let declaration = Declaration {
        schema_version: 1,
        operator: "synthetic".into(),
        root: format!("file://{}/governance", scratch.root.display())
            .parse()
            .unwrap(),
        namespace: "phase11".into(),
        populations: vec![],
    };
    let path = scratch.path("replay.toml");
    Fixture {
        scratch,
        config,
        path,
        declaration,
        datasets,
    }
}

fn replay(fixture: &mut Fixture, scenario: Option<ReplayScenario>) -> String {
    fixture.config.replay.as_mut().unwrap().scenario = scenario;
    fixture.save();
    let report = cli(
        &fixture.log(),
        &["replay", "--config", fixture.path.to_str().unwrap()],
    )
    .unwrap();
    common::generation(report.lines().next().unwrap())
}

#[test]
fn omitted_and_explicit_zero_delay_are_equivalent() {
    let mut fixture = small_replay("phase11_zero");
    let omitted = replay(&mut fixture, None);
    let zero = replay(
        &mut fixture,
        Some(ReplayScenario {
            schema_version: 1,
            id: "zero".into(),
            acceptance_delay_micros: 0,
        }),
    );
    assert_ne!(omitted, zero);
    let immediate_events = fixture.events(&omitted);
    let zero_events = fixture.events(&zero);
    assert_eq!(immediate_events.len(), zero_events.len());
    assert_ne!(immediate_events[0], zero_events[0]);
    assert_eq!(immediate_events[1..], zero_events[1..]);
    assert_eq!(fixture.summary(&omitted), fixture.summary(&zero));
    let summary = fixture.summary(&zero);
    assert_eq!(
        (
            summary.portfolio.accepted,
            summary.portfolio.settled,
            summary.portfolio.unresolved
        ),
        (5, 4, 1)
    );
    assert_eq!(summary.accounts[0].cash, decimal("9999.80"));
    assert_eq!(summary.accounts[1].cash, decimal("10002.40"));
}

#[test]
fn positive_delay_enters_at_the_causal_tick() {
    let mut fixture = small_replay("phase11_delay");
    for delay in [1_000_000, 1_125_000] {
        let generation = replay(
            &mut fixture,
            Some(ReplayScenario {
                schema_version: 1,
                id: format!("delay-{delay}"),
                acceptance_delay_micros: delay,
            }),
        );
        let events = fixture.events(&generation);
        let signals = events
            .iter()
            .filter_map(|event| match &event.kind {
                EventKind::Signal {
                    instrument,
                    command: Some(command),
                    ..
                } => Some((command.clone(), (instrument.clone(), event.time_micros))),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        let mut accepted = BTreeSet::new();
        for event in &events {
            if let EventKind::Accepted {
                command,
                entry_time_micros,
                entry_price_units,
                price_time_micros,
                due_time_micros,
                source,
                ..
            } = &event.kind
            {
                let (instrument, signal) = &signals[command];
                let index = usize::from(instrument == INSTRUMENTS[1]);
                let ticks = common::read_normalized_ticks(
                    &fixture.scratch.path("published"),
                    &fixture.datasets[index],
                );
                let expected_entry = signal + delay;
                let last = ticks
                    .iter()
                    .rev()
                    .find(|tick| tick.event_time_micros <= expected_entry)
                    .unwrap();
                assert_eq!(*entry_time_micros, Some(expected_entry));
                assert_eq!(*price_time_micros, Some(last.event_time_micros));
                assert_eq!(*entry_price_units, Some(last.price_units));
                assert_eq!(*due_time_micros, Some(expected_entry + 5_000_000));
                assert_eq!(source.available_at_micros, expected_entry);
                assert_eq!(source.provider_time_micros, last.event_time_micros);
                assert!(source.simulated);
                accepted.insert(command.clone());
            }
        }
        let last_a = signals
            .iter()
            .find(|(_, (instrument, at))| instrument == INSTRUMENTS[0] && *at == BASE + 40_000_000)
            .unwrap()
            .0;
        assert_eq!(
            accepted.contains(last_a),
            delay == 1_000_000,
            "response at the horizon is delivered, after it is not"
        );
        assert_eq!(signals.len(), 5);
        assert_eq!(accepted.len(), if delay == 1_000_000 { 5 } else { 4 });
        let unresolved = events
            .iter()
            .filter_map(|event| match &event.kind {
                EventKind::Unresolved {
                    command, reason, ..
                } => Some((command.clone(), reason.to_string())),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            unresolved,
            vec![(last_a.clone(), "window_exhausted".into())]
        );
        let summary = fixture.summary(&generation);
        assert_eq!(
            (summary.portfolio.settled, summary.portfolio.unresolved),
            (4, 1)
        );
        assert_eq!(
            summary.accounts[0].reserved,
            cents(if delay == 1_000_000 { 0 } else { 100 })
        );
        assert_eq!(
            summary.accounts[0].paid_basis,
            cents(if delay == 1_000_000 { 100 } else { 0 })
        );
        assert_eq!(summary.accounts[0].open, 1);
        assert!(
            events.last().unwrap().time_micros > BASE + 41_000_000,
            "B's later ticks never extend A's horizon"
        );
    }
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn point(alias: &Path, target: &Path) {
    if fs::symlink_metadata(alias).is_ok() {
        fs::remove_file(alias).unwrap();
    }
    std::os::unix::fs::symlink(target, alias).unwrap();
}

/// Independent stores share one logical URI via this test-only mount-point symlink. Paths
/// are configuration identity, so changing the physical scratch must not change those URIs.
struct Comparison {
    hub: Scratch,
    reference: PathBuf,
    initial: PathBuf,
    fixture: Fixture,
}
impl Comparison {
    fn new(name: &str) -> Self {
        let hub = Scratch::new(name);
        let reference = hub.path("uninterrupted");
        fs::create_dir_all(&reference).unwrap();
        let alias = hub.path("active");
        point(&alias, &reference);
        let fixture = Fixture::at(Scratch { root: alias }, PLANTED, PLANTED);
        let initial = hub.path("initial");
        copy_tree(&reference, &initial);
        Self {
            hub,
            reference,
            initial,
            fixture,
        }
    }
    fn restart(&self, name: &str, source: &Path) {
        let destination = self.hub.path(name);
        copy_tree(source, &destination);
        point(&self.fixture.scratch.root, &destination);
    }
}

fn count_files(path: &Path) -> usize {
    if !path.exists() {
        return 0;
    }
    fs::read_dir(path)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_file())
        .count()
}

/// Kills the run once the boundary is reached; whether the process was still running.
fn kill_at(
    fixture: &Fixture,
    ready_count: Option<usize>,
    governance: Option<(&str, usize)>,
    frozen: bool,
) -> bool {
    fs::write(fixture.log(), []).unwrap();
    let stderr = fs::File::create(fixture.scratch.path("interrupted.stderr")).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args([
            "research",
            "run",
            "--config",
            fixture.path.to_str().unwrap(),
        ])
        .env("BINARY_ALPHA_STORE_LOG", fixture.log())
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .unwrap();
    let started = Instant::now();
    loop {
        let reached = ready_count
            .is_some_and(|count| fixture.scratch.manifests("published").len() >= count)
            || governance
                .is_some_and(|(dir, count)| count_files(&fixture.governance_path(dir)) >= count)
            || (frozen
                && fixture
                    .scratch
                    .path("published")
                    .join(research::frozen_key(&fixture.generation()))
                    .is_file());
        if reached {
            break;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "run ended before interruption: {}",
            fs::read_to_string(fixture.scratch.path("interrupted.stderr")).unwrap()
        );
        if started.elapsed() > Duration::from_secs(30) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("stage deadline exceeded");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    child.kill().unwrap();
    !child.wait().unwrap().success()
}

#[test]
fn interrupted_runs_resume_to_identical_results() {
    let comparison = Comparison::new("phase11_interruptions");
    let fixture = &comparison.fixture;
    fixture.run().unwrap();
    let development_log = logged(&fixture.log());
    let (manifest, run) = fixture.run_record();
    let run_bytes = fixture.object(&manifest.generation, "research.json");
    let (.., grant) = fixture.grant();
    let grant_bytes = fs::read(fixture.grant_path()).unwrap();
    fixture.run().unwrap();
    let (certification, record) = fixture.certification(&grant);
    let certification_bytes = fixture.object(&certification.generation, "certification.json");
    let initial_count = fs::read_dir(comparison.initial.join("published/manifests"))
        .unwrap()
        .count();
    let mut published = Vec::new();
    for line in &development_log {
        if let Some(key) = line
            .strip_prefix("put_new manifests/")
            .and_then(|k| k.strip_suffix("/ready.json"))
            && !published.iter().any(|g| g == key)
        {
            published.push(key.to_string());
        }
    }
    let boundary = |kind: &str| {
        initial_count
            + published
                .iter()
                .position(|g| fixture.manifest(g)["kind"] == kind)
                .unwrap()
            + 1
    };
    let profile = boundary("instrument_stream");
    let family = boundary("search_family");
    let selection = boundary("portfolio_selection");
    let final_run = boundary("research_run");
    let frozen_bytes = fs::read(
        fixture
            .scratch
            .path("published")
            .join(research::frozen_key(&fixture.generation())),
    )
    .unwrap();
    for (name, count, governance, frozen) in [
        ("profile", Some(profile), None, false),
        ("family", Some(family), None, false),
        ("selection", Some(selection), None, false),
        ("frozen", None, None, true),
        ("run", Some(final_run), None, false),
        (
            "assessment-claim",
            None,
            Some(("phase11/assessment-use", 1)),
            false,
        ),
        ("receipt", None, Some(("phase11/receipts", 1)), false),
    ] {
        comparison.restart(name, &comparison.initial);
        if name == "receipt" {
            fixture.run().unwrap();
            write(&fixture.grant_path(), &grant_bytes);
        }
        // The run manifest is the last durable effect of the run at the destination: the ready
        // file can appear as the process exits, and a completed run resumes through the same
        // path as an interrupted one.
        let interrupted = kill_at(fixture, count, governance, frozen);
        assert!(
            interrupted || name == "run",
            "{name}: actual process termination is required"
        );
        let before = manifest_snapshot(&fixture.scratch.path("published"));
        let retained = manifest_snapshot(&fixture.scratch.path("retained"));
        if name == "profile" || name == "family" {
            no_access(&logged(&fixture.log()), &fixture.evaluation());
            no_access(&logged(&fixture.log()), &fixture.protected());
        }
        fixture.run().unwrap();
        assert_eq!(
            fixture.run_record(),
            (manifest.clone(), run.clone()),
            "{name}"
        );
        if !matches!(name, "profile" | "family" | "selection") {
            // A published frozen stage restores its children through their verifiers: nothing
            // frozen is recomputed or republished after the freeze.
            let frozen_children: Vec<String> = run
                .instruments
                .iter()
                .flat_map(|record| {
                    [
                        record.profile.clone(),
                        record.feature.clone(),
                        record.outcome.clone(),
                        record.family.clone(),
                    ]
                })
                .chain(std::iter::once(run.selection.clone()))
                .map(|generation| format!("put_new manifests/{generation}/ready.json"))
                .collect();
            let resumed = logged(&fixture.log());
            assert!(
                resumed.iter().all(|line| !frozen_children.contains(line)),
                "{name}: a frozen child was republished"
            );
        }
        assert_eq!(
            fixture.object(&manifest.generation, "research.json"),
            run_bytes,
            "{name}"
        );
        assert_eq!(
            fs::read(
                fixture
                    .scratch
                    .path("published")
                    .join(research::frozen_key(&fixture.generation()))
            )
            .unwrap(),
            frozen_bytes
        );
        if name != "receipt" {
            write(&fixture.grant_path(), &grant_bytes);
            fixture.run().unwrap();
        }
        assert_eq!(
            fixture.certification(&grant),
            (certification.clone(), record.clone()),
            "{name}"
        );
        assert_eq!(
            fixture.object(&certification.generation, "certification.json"),
            certification_bytes,
            "{name}"
        );
        assert_manifest_snapshot(&fixture.scratch.path("published"), &before);
        assert_manifest_snapshot(&fixture.scratch.path("retained"), &retained);
    }
}

#[test]
fn partial_claims_are_retained_and_conflicts_deny_protected_access() {
    let comparison = Comparison::new("phase11_claim_conflicts");
    let fixture = &comparison.fixture;
    fixture.run().unwrap();
    let (manifest, run) = fixture.run_record();
    let assessment_claims = run
        .claims
        .iter()
        .map(|k| (k.clone(), fs::read(fixture.governance_path(k)).unwrap()))
        .collect::<Vec<_>>();
    let awaiting = comparison.hub.path("awaiting");
    copy_tree(&comparison.reference, &awaiting);
    let (_, grant) = fixture.grant();
    let grant_bytes = research::to_json(&grant);
    fixture.run().unwrap();
    let (_, certified) = fixture.certification(&grant);
    let claims = certified
        .claims
        .iter()
        .map(|k| (k.clone(), fs::read(fixture.governance_path(k)).unwrap()))
        .collect::<Vec<_>>();
    let receipt_bytes = fs::read(fixture.governance_path(&certified.receipt)).unwrap();

    comparison.restart("assessment-matching", &comparison.initial);
    write(
        &fixture.governance_path(&assessment_claims[0].0),
        &assessment_claims[0].1,
    );
    fixture.run().unwrap();
    assert_eq!(fixture.run_record(), (manifest.clone(), run.clone()));
    assert_claims(fixture, &run, ClaimKind::AssessmentUse, None);

    comparison.restart("assessment-conflict", &comparison.initial);
    let mut conflict: Claim = serde_json::from_slice(&assessment_claims[1].1).unwrap();
    conflict.attempt = "competing".into();
    let conflict_bytes = research::to_json(&conflict);
    write(
        &fixture.governance_path(&assessment_claims[1].0),
        &conflict_bytes,
    );
    let error = fixture.run().unwrap_err();
    assert!(
        error.contains("population token `evaluation-0-b` is claimed by another assessment"),
        "{error}"
    );
    no_access(&logged(&fixture.log()), &fixture.evaluation());
    no_access(&logged(&fixture.log()), &fixture.protected());
    assert_eq!(
        fs::read(fixture.governance_path(&assessment_claims[0].0)).unwrap(),
        assessment_claims[0].1
    );
    assert_eq!(
        fs::read(fixture.governance_path(&assessment_claims[1].0)).unwrap(),
        conflict_bytes
    );
    assert_eq!(
        count_files(&fixture.governance_path("phase11/assessment-use")),
        2
    );

    comparison.restart("holdout-conflict", &awaiting);
    write(&fixture.grant_path(), &grant_bytes);
    let mut conflict: Claim = serde_json::from_slice(&claims[1].1).unwrap();
    conflict.research = "other-run".into();
    write(
        &fixture.governance_path(&claims[1].0),
        research::to_json(&conflict),
    );
    let error = fixture.run().unwrap_err();
    assert!(
        error.contains("population token `holdout-0-b` is claimed by another assessment"),
        "{error}"
    );
    no_access(&logged(&fixture.log()), &fixture.protected());
    assert_eq!(
        fs::read(fixture.governance_path(&claims[0].0)).unwrap(),
        claims[0].1
    );
    assert_eq!(
        count_files(&fixture.governance_path("phase11/holdout-use")),
        2
    );
    assert_eq!(count_files(&fixture.governance_path("phase11/receipts")), 0);

    comparison.restart("receipt-conflict", &awaiting);
    write(&fixture.grant_path(), &grant_bytes);
    let mut receipt = Receipt::from_json(&receipt_bytes).unwrap();
    receipt.research = "another-run".into();
    write(
        &fixture.governance_path(&certified.receipt),
        research::to_json(&receipt),
    );
    let error = fixture.run().unwrap_err();
    assert!(
        error.contains("the grant is consumed by another run"),
        "{error}"
    );
    no_access(&logged(&fixture.log()), &fixture.protected());
    assert_eq!(
        fs::read(fixture.governance_path(&certified.receipt)).unwrap(),
        research::to_json(&receipt)
    );
    assert_eq!(
        count_files(&fixture.governance_path("phase11/holdout-use")),
        4
    );

    for change in ["bundle", "holdout"] {
        comparison.restart(&format!("grant-{change}"), &awaiting);
        let mut wrong = grant.clone();
        if change == "bundle" {
            wrong.bundle_sha256 = "f".repeat(64);
        } else {
            wrong.holdout[0].manifest = uri(
                &fixture.scratch.root,
                &fixture.declaration.populations[8].generations[1],
            );
        }
        wrong.hash = wrong.content_hash();
        write(&fixture.grant_path(), research::to_json(&wrong));
        let error = fixture.run().unwrap_err();
        assert!(
            error.contains(
                "does not authorize this run's frozen bundle over the declared holdout population"
            ),
            "{error}"
        );
        no_access(&logged(&fixture.log()), &fixture.protected());
        assert_eq!(
            count_files(&fixture.governance_path("phase11/holdout-use")),
            0
        );
        assert_eq!(count_files(&fixture.governance_path("phase11/receipts")), 0);
    }

    // An altered frozen stage never reaches authorization: the run refuses to resume, the grant
    // command refuses to create authority, and no holdout key is touched.
    comparison.restart("altered-bundle", &awaiting);
    let frozen_path = fixture
        .scratch
        .path("published")
        .join(research::frozen_key(&fixture.generation()));
    let mut altered = fs::read(&frozen_path).unwrap();
    altered.push(b' ');
    write(&frozen_path, &altered);
    let error = fixture.run().unwrap_err();
    assert!(error.contains("the frozen stage does not bind"), "{error}");
    no_access(&logged(&fixture.log()), &fixture.protected());
    let holdout = &fixture.config.research.as_ref().unwrap().holdout.inputs;
    let error = cli_as(
        &fixture.log(),
        OPERATOR,
        &[
            "holdout",
            "grant",
            "create",
            "--config",
            fixture.path.to_str().unwrap(),
            "--bundle-manifest",
            &uri(&fixture.scratch.root, &fixture.generation()).to_string(),
            "--holdout-manifest",
            &holdout[0].to_string(),
            "--holdout-manifest",
            &holdout[1].to_string(),
            "--reason",
            "altered bundle",
        ],
    )
    .unwrap_err();
    assert!(error.contains("the frozen stage does not bind"), "{error}");
    assert!(!fixture.grant_path().exists());
    no_access(&logged(&fixture.log()), &fixture.protected());

    comparison.restart("second-attempt", &awaiting);
    let mut changed = fixture.config.clone();
    let settings = changed.research.as_mut().unwrap();
    settings.study.attempt = "second".into();
    settings.study.predecessors = vec!["first".into()];
    settings.study.changes = "different declared delay".into();
    settings.scenarios[0].acceptance_delay_micros = 125_000;
    let changed_path = fixture.scratch.path("second.toml");
    write(&changed_path, changed.canonical_toml());
    let error = cli(
        &fixture.log(),
        &[
            "research",
            "run",
            "--config",
            changed_path.to_str().unwrap(),
        ],
    )
    .unwrap_err();
    assert!(
        error.contains("population token `evaluation-0-a` is claimed by another assessment"),
        "{error}"
    );
    no_access(&logged(&fixture.log()), &fixture.evaluation());
    no_access(&logged(&fixture.log()), &fixture.protected());
    for (key, bytes) in &assessment_claims {
        assert_eq!(fs::read(fixture.governance_path(key)).unwrap(), *bytes);
    }

    let mut moved = fixture.declaration.clone();
    moved.root = format!(
        "file://{}",
        fixture.scratch.path("new-governance").display()
    )
    .parse()
    .unwrap();
    let new_declaration = fixture.scratch.path("moved.json");
    write(&new_declaration, research::to_json(&moved));
    changed.research.as_mut().unwrap().study.governance_manifest =
        format!("file://{}", new_declaration.display());
    write(&changed_path, changed.canonical_toml());
    let error = cli(
        &fixture.log(),
        &[
            "research",
            "run",
            "--config",
            changed_path.to_str().unwrap(),
        ],
    )
    .unwrap_err();
    assert!(
        error.contains("study.predecessors: attempt `first` has no intent at"),
        "{error}"
    );
    no_access(
        &logged(&fixture.log()),
        &fixture
            .datasets
            .iter()
            .map(|d| d.generation.clone())
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        count_files(
            &fixture
                .scratch
                .path("new-governance/phase11/assessment-use")
        ),
        0
    );
}

#[test]
fn ordinary_entry_points_are_denied_before_protected_access() {
    let fixture = Fixture::new("phase11_ordinary_guards");
    fixture.run().unwrap();
    let (_, run) = fixture.run_record();
    let holdout = &fixture.config.research.as_ref().unwrap().holdout.inputs[0];
    let first = &fixture.config.research.as_ref().unwrap().instruments[0];
    let profile = uri(&fixture.scratch.root, &run.instruments[0].profile);
    let feature = uri(&fixture.scratch.root, &run.instruments[0].feature);
    let mut feature_config = fixture.config.clone();
    feature_config.features = Some(binary_alpha_engine::config::Features {
        instruments: vec![research::fit_entry(first, holdout, profile)],
    });
    let mut outcome_config = fixture.config.clone();
    outcome_config.outcomes = Some(research::outcomes_table(first, holdout, &feature));
    let mut replay_config = fixture_config::replay_configuration(&fixture.scratch.root);
    replay_config.research = fixture.config.research.clone();
    replay_config.replay.as_mut().unwrap().inputs[0].tick_manifest = holdout.clone();
    let mut explicit = replay_config.clone();
    explicit.replay.as_mut().unwrap().role = DatasetRole::Holdout;
    for governed in [true, false] {
        for (name, mut config, args, message) in [
            (
                "verify",
                fixture.config.clone(),
                vec!["data", "verify"],
                PROTECTED,
            ),
            (
                "audit",
                fixture.config.clone(),
                vec!["data", "audit"],
                if governed {
                    PROTECTED
                } else {
                    "research never audits holdout data"
                },
            ),
            (
                "features",
                feature_config.clone(),
                vec!["features", "build"],
                if governed {
                    "is declared `holdout`, not `development`"
                } else {
                    PROTECTED
                },
            ),
            (
                "outcomes",
                outcome_config.clone(),
                vec!["outcomes", "build"],
                if governed {
                    "is declared `holdout`, not `development`"
                } else {
                    PROTECTED
                },
            ),
            (
                "replay",
                replay_config.clone(),
                vec!["replay"],
                if governed {
                    "is declared `holdout`, not `development`"
                } else {
                    PROTECTED
                },
            ),
            (
                "explicit-holdout",
                explicit.clone(),
                vec!["replay"],
                "holdout data never enters a replay",
            ),
        ] {
            if !governed {
                config.research = None;
            }
            let path = fixture
                .scratch
                .path(&format!("ordinary-{name}-{governed}.toml"));
            write(&path, config.canonical_toml());
            let mut args = args;
            args.extend(["--config", path.to_str().unwrap()]);
            let manifest_uri = holdout.to_string();
            if name == "verify" || name == "audit" {
                args.extend(["--manifest", &manifest_uri]);
            }
            let result = cli(&fixture.log(), &args).unwrap_err();
            let log = logged(&fixture.log());
            if governed || name == "explicit-holdout" {
                no_access(&log, &fixture.protected());
            } else {
                assert_eq!(
                    log,
                    vec![format!("read_to {}", holdout.key)],
                    "{name}: only the manifest may be read"
                );
            }
            assert!(result.contains(message), "{name}/{governed}: {result}");
        }
    }
}

#[test]
fn holdout_feature_reuse_requires_matching_certification_context() {
    let comparison = Comparison::new("phase11_holdout_feature_reuse");
    let fixture = &comparison.fixture;
    fixture.run().unwrap();
    let (_, run) = fixture.run_record();
    let (_, grant) = fixture.grant();
    fixture.run().unwrap();
    let (certification, record) = fixture.certification(&grant);
    let features = &record.scenarios[0].outer.features;
    assert_eq!(features.len(), 2);

    // A new physical copy retains the original completed synthetic evidence. With only its
    // certification ready marker absent, the same grant enters assessment and either reuses
    // each verified holdout feature or rebuilds it when the producer revision is ambiguous.
    comparison.restart("authorized-reuse", &comparison.reference);
    fs::remove_file(
        fixture
            .published()
            .join(manifest_key(&certification.generation)),
    )
    .unwrap();
    fixture.run().unwrap();
    let (reused_certification, reused_record) = fixture.certification(&grant);
    assert_eq!(reused_certification, certification);
    assert_eq!(reused_record, record);
    let log = logged(&fixture.log());
    let input_keys: Vec<_> = fixture
        .config
        .research
        .as_ref()
        .unwrap()
        .holdout
        .inputs
        .iter()
        .flat_map(|input| {
            let dataset = GenerationManifest::from_json(
                &fs::read(fixture.published().join(&input.key)).unwrap(),
            )
            .unwrap();
            dataset.objects.into_iter().map(|object| object.key)
        })
        .collect();
    let first_input = log.iter().position(|line| {
        input_keys
            .iter()
            .any(|key| line == &format!("read_to {key}"))
    });
    let reusable_revision = !env!("BINARY_ALPHA_CODE_REVISION").ends_with("-dirty")
        && env!("BINARY_ALPHA_CODE_REVISION") != "unavailable";
    for feature in features {
        let key = manifest_key(&feature.generation);
        assert!(log.iter().any(|line| line == &format!("read_to {key}")));
        let manifest =
            FeatureManifest::from_json(&fs::read(fixture.published().join(key)).unwrap()).unwrap();
        let plan = manifest
            .objects
            .iter()
            .find(|object| object.path == "plan.json")
            .unwrap();
        let plan_read = log
            .iter()
            .position(|line| line == &format!("read_to {}", plan.key))
            .unwrap();
        if let Some(first_input) = first_input {
            assert!(
                plan_read < first_input,
                "feature verification preceded holdout streaming"
            );
        }
        for object in &manifest.objects {
            let published_again = log
                .iter()
                .any(|line| line == &format!("put_new {}", object.key));
            assert_eq!(
                published_again, !reusable_revision,
                "feature object {} reuse depended on producer revision",
                object.key
            );
        }
    }

    let input = &fixture.config.research.as_ref().unwrap().holdout.inputs[0];
    let entry: binary_alpha_engine::config::FeatureInstrument =
        serde_json::from_value(serde_json::json!({
            "role": "holdout",
            "input_manifest": input,
            "profile_manifest": uri(&fixture.scratch.root, &run.instruments[0].profile),
            "frozen_plan": uri(&fixture.scratch.root, &run.instruments[0].feature),
        }))
        .unwrap();
    let mut ordinary = fixture.config.clone();
    ordinary.features = Some(binary_alpha_engine::config::Features {
        instruments: vec![entry],
    });
    let path = fixture.scratch.path("denied-holdout-feature-reuse.toml");
    write(&path, ordinary.canonical_toml());
    let error = cli(
        &fixture.log(),
        &["features", "build", "--config", path.to_str().unwrap()],
    )
    .unwrap_err();
    assert!(
        error.contains("holdout data is never a feature-build input"),
        "{error}"
    );
    let denied = logged(&fixture.log());
    assert!(!denied.iter().any(|line| line.contains("features/fits/")));
    no_access(&denied, &fixture.protected());
    for feature in features {
        let manifest = fixture.manifest(&feature.generation);
        for object in manifest["objects"].as_array().unwrap() {
            let key = object["key"].as_str().unwrap();
            assert!(!denied.iter().any(|line| line.contains(key)));
        }
    }
}

#[test]
fn research_configuration_is_validated_before_any_read() {
    let fixture = Fixture::new("phase11_invalid");
    let original = fixture.config.clone();
    let mut cases = Vec::new();
    let mut bad = original.clone();
    bad.research.as_mut().unwrap().scenarios[0]
        .alternatives
        .pop();
    cases.push((bad, "every portfolio binding needs exactly one alternative"));
    let mut bad = original.clone();
    bad.research.as_mut().unwrap().scenarios[0].id = "baseline".into();
    cases.push((bad, "`baseline` is the baseline or is listed twice"));
    let mut bad = original.clone();
    bad.research.as_mut().unwrap().scenarios[0].acceptance_delay_micros = -1;
    cases.push((bad, "acceptance_delay_micros: must be non-negative"));
    let mut bad = original.clone();
    bad.research.as_mut().unwrap().qualification.claim = "future-profit".into();
    cases.push((
        bad,
        "not the supported claim `empirical_policy_qualification_v1`",
    ));
    let mut bad = original.clone();
    bad.research.as_mut().unwrap().holdout.inputs.pop();
    cases.push((bad, "holdout.inputs: 1 entries for 2 instruments"));
    let mut bad = original.clone();
    bad.research.as_mut().unwrap().instruments[0].source_manifest =
        uri(&fixture.scratch.root, &"e".repeat(64));
    cases.push((bad, "is not declared by the governance declaration"));
    let mut bad = original.clone();
    bad.research.as_mut().unwrap().instruments[0].source_manifest =
        bad.research.as_ref().unwrap().holdout.inputs[0].clone();
    cases.push((bad, "is declared `holdout`, not `development`"));
    for (i, (config, message)) in cases.into_iter().enumerate() {
        let path = fixture.scratch.path(&format!("invalid-{i}.toml"));
        write(&path, config.canonical_toml());
        let result = cli(
            &fixture.log(),
            &["research", "run", "--config", path.to_str().unwrap()],
        )
        .unwrap_err();
        assert!(result.contains(message), "{i}: {result}");
        assert_no_dataset_read(&fixture, &logged(&fixture.log()));
        assert_eq!(
            count_files(&fixture.governance_path("phase11/assessment-use")),
            0
        );
        assert_eq!(
            count_files(&fixture.governance_path("phase11/holdout-use")),
            0
        );
    }
    for name in ["shared-token", "wrong-holdout-role"] {
        let mut declaration = fixture.declaration.clone();
        let message = if name == "shared-token" {
            declaration.populations[8].tokens = declaration.populations[0].tokens.clone();
            "a protected token is never exposed to development or evaluation"
        } else {
            declaration.populations[8].role = DatasetRole::Development;
            "is declared `development`, not `holdout`"
        };
        write(
            &fixture.scratch.path("declaration.json"),
            research::to_json(&declaration),
        );
        let error = fixture.run().unwrap_err();
        assert!(error.contains(message), "{error}");
        assert_no_dataset_read(&fixture, &logged(&fixture.log()));
    }
}

fn assert_no_dataset_read(fixture: &Fixture, log: &[String]) {
    for line in log {
        assert!(!line.contains("objects/"), "{line}");
        assert!(!line.starts_with("read_to manifests/"), "{line}");
        for dataset in &fixture.datasets {
            assert!(!line.contains(&dataset.generation), "{line}");
        }
    }
}

#[test]
fn economic_failure_and_insufficient_evidence_are_terminal() {
    for failure in [
        "outer-economics",
        "outer-support",
        "holdout-economics",
        "holdout-support",
    ] {
        let mut fixture = Fixture::at(
            Scratch::new(&format!("phase11_{failure}")),
            PLANTED,
            if failure == "holdout-economics" {
                LOSING
            } else {
                PLANTED
            },
        );
        if failure == "outer-economics" {
            let scenario = &mut fixture.config.research.as_mut().unwrap().scenarios[1];
            for alternative in &mut scenario.alternatives {
                alternative.contract.win.gross_return = decimal("2.06");
                alternative.envelope.min_winning_net_return = decimal("0.01");
            }
        }
        if failure == "outer-support" {
            fixture
                .config
                .research
                .as_mut()
                .unwrap()
                .qualification
                .gates
                .min_settled = 17;
        }
        if failure == "holdout-support" {
            // A fully computed holdout with only wide candles supplies no eligible narrow
            // trades. The frozen horizon and all development/evaluation observations stay fixed.
            let mut rows = recipe(PLANTED);
            for row in &mut rows {
                row.wide = true;
            }
            let different = import_pair(
                &fixture.scratch,
                "holdout-no-support",
                DatasetRole::Holdout,
                BASE + 4 * HOUR,
                &rows,
            );
            for (i, dataset) in different.into_iter().enumerate() {
                fixture.config.research.as_mut().unwrap().holdout.inputs[i] =
                    uri(&fixture.scratch.root, &dataset.generation);
                fixture.declaration.populations[8 + i]
                    .generations
                    .push(dataset.generation.clone());
                fixture.datasets.push(dataset);
            }
        }
        fixture.save();
        fixture.run().unwrap();
        let (manifest, run) = fixture.run_record();
        let selection = fixture.selection(&run);
        assert_eq!(selection.choices[selection.selected.unwrap()].subset, 2);
        assert_eq!(
            selection.choices[selection.selected.unwrap()].alternatives,
            [1, 1]
        );
        let (verdict, results) = if failure.starts_with("outer") {
            let RunState::OuterRejected { verdict } = &run.state else {
                panic!("{}", manifest.state)
            };
            assert_eq!(manifest.state, "outer_rejected");
            no_access(&logged(&fixture.log()), &fixture.protected());
            assert_eq!(
                count_files(&fixture.governance_path("phase11/holdout-use")),
                0
            );
            let before = manifest_snapshot(&fixture.scratch.path("published"));
            fixture.run().unwrap();
            assert_manifest_snapshot(&fixture.scratch.path("published"), &before);
            (verdict.clone(), run.outer.clone())
        } else {
            assert_eq!(run.state, RunState::AwaitingHoldoutAuthorization);
            let (_, grant) = fixture.grant();
            fixture.run().unwrap();
            let (manifest, record) = fixture.certification(&grant);
            assert_eq!(manifest.state, "rejected");
            assert_eq!(record.scenarios.len(), 3);
            assert_eq!(
                count_files(&fixture.governance_path("phase11/holdout-use")),
                4
            );
            assert_eq!(count_files(&fixture.governance_path("phase11/receipts")), 1);
            let before = manifest_snapshot(&fixture.scratch.path("published"));
            fixture.run().unwrap();
            assert_manifest_snapshot(&fixture.scratch.path("published"), &before);
            assert_eq!(fixture.certification(&grant), (manifest, record.clone()));
            (record.verdict, record.scenarios)
        };
        assert_eq!(
            verdict.reason(),
            Some(if failure.ends_with("support") {
                "insufficient_evidence"
            } else {
                "economic_failure"
            })
        );
        match failure {
            "outer-economics" => {
                assert_eq!(results[0].verdict, Verdict::Pass);
                assert_eq!(results[1].verdict, Verdict::Pass);
                assert_eq!(results[0].outer.projection.profit, Some(decimal("8.40")));
                assert_eq!(results[2].outer.projection.profit, Some(decimal("-16.24")));
                assert_eq!(
                    results[2].verdict,
                    Verdict::EconomicFailure {
                        reason: "net profit -16.24 below the minimum 0".into()
                    }
                );
            }
            "holdout-economics" => {
                let expected = joint(
                    &recipe(LOSING),
                    &[(0, true, true, true), (1, false, true, true)],
                    false,
                );
                assert_eq!(results[0].outer.projection.profit, Some(cents(expected.0)));
                assert_eq!(
                    results[0].outer.projection.drawdown,
                    Some(cents(expected.1))
                );
                assert_eq!(results[0].outer.projection.settled, 16);
            }
            _ => {
                for result in &results {
                    let expected = if failure == "outer-support" { 16 } else { 0 };
                    assert_eq!(result.outer.projection.settled, expected);
                    assert_eq!(result.outer.projection.profit, None);
                    assert_eq!(
                        result.verdict,
                        Verdict::InsufficientEvidence {
                            reason: format!(
                                "settled {expected} below the minimum {}",
                                if failure == "outer-support" { 17 } else { 1 }
                            )
                        }
                    );
                }
            }
        }
    }
}

#[test]
fn development_is_independent_of_later_observations() {
    let mut comparison = Comparison::new("phase11_independence");
    let fixture = &mut comparison.fixture;
    let evaluation = import_pair(
        &fixture.scratch,
        "changed-evaluation",
        DatasetRole::Evaluation,
        BASE + 3 * HOUR,
        &recipe(LOSING),
    );
    let holdout = import_pair(
        &fixture.scratch,
        "changed-holdout",
        DatasetRole::Holdout,
        BASE + 4 * HOUR,
        &recipe(LOSING),
    );
    for (offset, variants) in [(6, &evaluation), (8, &holdout)] {
        for (i, dataset) in variants.iter().enumerate() {
            fixture.declaration.populations[offset + i]
                .generations
                .push(dataset.generation.clone());
        }
    }
    fixture.save();
    let prepared = comparison.hub.path("prepared");
    copy_tree(&comparison.reference, &prepared);
    let original_config = fixture.config.clone();
    fixture.run().unwrap();
    let (manifest, run) = fixture.run_record();
    let selection_bytes = fixture.object(&run.selection, "selection.json");
    let frozen = fs::read(
        fixture
            .scratch
            .path("published")
            .join(research::frozen_key(&fixture.generation())),
    )
    .unwrap();
    let baseline_frozen = Frozen::from_json(&frozen).unwrap();
    let development = run
        .instruments
        .iter()
        .flat_map(|record| {
            [
                record.profile.clone(),
                record.feature.clone(),
                record.outcome.clone(),
                record.family.clone(),
            ]
        })
        .chain(std::iter::once(run.selection.clone()))
        .map(|generation| {
            let path = fixture
                .scratch
                .path("published")
                .join(manifest_key(&generation));
            let manifest = fixture.manifest(&generation);
            let objects = manifest["objects"]
                .as_array()
                .unwrap()
                .iter()
                .map(|object| {
                    let key = object["key"].as_str().unwrap().to_owned();
                    let bytes = fs::read(fixture.scratch.path("published").join(&key)).unwrap();
                    (key, bytes)
                })
                .collect::<Vec<_>>();
            (generation, fs::read(path).unwrap(), objects)
        })
        .collect::<Vec<_>>();
    let (_, grant) = fixture.grant();
    fixture.run().unwrap();
    let (_, baseline_certification) = fixture.certification(&grant);
    assert_eq!(baseline_certification.verdict, Verdict::Pass);

    for (name, variants) in [
        ("evaluation-variant", evaluation),
        ("holdout-variant", holdout),
    ] {
        let destination = comparison.hub.path(name);
        copy_tree(&prepared, &destination);
        point(&fixture.scratch.root, &destination);
        fixture.config = original_config.clone();
        let settings = fixture.config.research.as_mut().unwrap();
        let inputs = if name == "evaluation-variant" {
            &mut settings.evaluation.inputs
        } else {
            &mut settings.holdout.inputs
        };
        for (input, dataset) in inputs.iter_mut().zip(&variants) {
            *input = uri(&fixture.scratch.root, &dataset.generation);
        }
        fixture.save();
        fixture.run().unwrap();
        let (changed_manifest, changed) = fixture.run_record();
        assert_ne!(
            manifest.generation, changed_manifest.generation,
            "declared input identities bind the enclosing run"
        );
        assert_eq!(changed.instruments, run.instruments);
        assert_eq!(changed.selection, run.selection);
        assert_eq!(
            fixture.object(&changed.selection, "selection.json"),
            selection_bytes
        );
        assert_eq!(changed.descriptor, run.descriptor);
        for (generation, bytes, objects) in &development {
            assert_eq!(
                fs::read(
                    fixture
                        .scratch
                        .path("published")
                        .join(manifest_key(generation))
                )
                .unwrap(),
                *bytes
            );
            for (key, bytes) in objects {
                assert_eq!(
                    fs::read(fixture.scratch.path("published").join(key)).unwrap(),
                    *bytes
                );
            }
        }
        let changed_bytes = fs::read(
            fixture
                .scratch
                .path("published")
                .join(research::frozen_key(&changed_manifest.generation)),
        )
        .unwrap();
        assert_ne!(changed_bytes, frozen);
        let changed_frozen = Frozen::from_json(&changed_bytes).unwrap();
        // Only the enclosing run identity changes. Every frozen scientific value and every
        // referenced development byte must remain identical. A literal byte-equal enclosing
        // stage would incorrectly erase the changed input reference from the run's identity.
        assert_eq!(
            Frozen {
                research: baseline_frozen.research.clone(),
                ..changed_frozen
            },
            baseline_frozen
        );
        if name == "evaluation-variant" {
            assert_ne!(changed.outer, run.outer);
            assert!(matches!(
                changed.state,
                RunState::OuterRejected {
                    verdict: Verdict::EconomicFailure { .. }
                }
            ));
            no_access(&logged(&fixture.log()), &fixture.protected());
        } else {
            assert_eq!(changed.outer, run.outer);
            assert_eq!(changed.state, run.state);
            let (_, grant) = fixture.grant();
            fixture.run().unwrap();
            let (envelope, record) = fixture.certification(&grant);
            assert_eq!(envelope.state, "rejected");
            assert_eq!(record.verdict.reason(), Some("economic_failure"));
            assert_ne!(record.scenarios, baseline_certification.scenarios);
            assert_eq!(
                fixture.object(&changed.selection, "selection.json"),
                selection_bytes
            );
        }
    }
}
