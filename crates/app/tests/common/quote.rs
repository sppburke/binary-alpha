//! Synthetic quote research fixture shared by certification and live replay proofs.
use crate::common::Scratch;
use crate::fixture_config::{self, BASE, CURRENCIES, HOUR, INSTRUMENTS, SYMBOLS, time, uri};
use binary_alpha_engine::config::{Config, SearchCondition, StreamKey};
use binary_alpha_engine::dataset::{DatasetRole, GenerationManifest};
use binary_alpha_engine::execution::{Comparator, Decimal, Threshold};
use binary_alpha_engine::research::{Declaration, Population};

fn decimal(value: &str) -> Decimal {
    Decimal::parse(value).unwrap()
}

pub fn build(scratch: &Scratch) -> (Config, Declaration, Vec<GenerationManifest>) {
    use binary_alpha_engine::config::{
        Deployment, NamedSearchCondition, Outputs, PortfolioMember, ScenarioAlternative, Subset,
    };
    use binary_alpha_engine::execution::{ContractTerms, Direction};
    use binary_alpha_engine::market::PriceScale;
    let mut config = fixture_config::configuration(&scratch.root);
    for instrument in &mut config.instruments {
        instrument.price_scale = PriceScale::try_from(5).unwrap();
    }
    let mut datasets = Vec::new();
    let mut populations = Vec::new();
    let mut refs = Vec::new();
    for (hour, name, role) in [
        (0, "source", DatasetRole::Development),
        (1, "assessment", DatasetRole::Development),
        (2, "refit", DatasetRole::Development),
        (3, "evaluation", DatasetRole::Evaluation),
        (4, "holdout", DatasetRole::Holdout),
    ] {
        let lines = quote_ticks(BASE + hour * HOUR);
        let imported = fixture_config::import_ticks(
            &scratch.root,
            name,
            role,
            "pocket_option",
            &SYMBOLS,
            &[5, 5],
            &[lines.clone(), lines],
        );
        for (i, manifest) in imported.into_iter().enumerate() {
            refs.push(uri(&scratch.root, &manifest.generation));
            populations.push(Population {
                id: format!("{name}-{i}"),
                role,
                instrument: INSTRUMENTS[i].into(),
                source: "invented-scale-five-quotes-v1".into(),
                coverage: manifest.coverage.clone(),
                generations: vec![manifest.generation.clone()],
                tokens: vec![format!("{name}-{i}-a"), format!("{name}-{i}-b")],
                exposure: vec![],
            });
            datasets.push(manifest);
        }
    }
    let declaration = Declaration {
        schema_version: 1,
        operator: "synthetic-operator".into(),
        root: format!("file://{}/governance", scratch.root.display())
            .parse()
            .unwrap(),
        namespace: "phase11-quote".into(),
        populations,
    };
    let quote = StreamKey::quote();
    let contract = |i: usize, direction: Direction| -> ContractTerms {
        let mut terms: ContractTerms = serde_json::from_value(fixture_config::contract(
            &format!("{i}-{direction}"),
            if i == 0 { "USD" } else { CURRENCIES[i] },
            false,
            false,
        ))
        .unwrap();
        terms.direction = direction;
        terms.duration_micros = 30_000_000;
        terms.win.gross_return = decimal("1.92");
        terms.tie.gross_return = decimal("0");
        terms.settlement.max_settlement_delay_micros = 1_000_000;
        terms.settlement.max_tick_gap_micros = 2_000_000;
        terms
    };
    let envelope = || {
        let mut value: binary_alpha_engine::execution::Envelope =
            serde_json::from_value(fixture_config::envelope(false, false)).unwrap();
        value.min_winning_net_return = decimal("0.92");
        value
    };
    let cutoff = |hour| time(BASE + hour * HOUR + 32 * 40_000_000);
    let start = |hour| time(BASE + hour * HOUR);
    let end = |hour| time(BASE + hour * HOUR + 32 * 40_000_000);
    let research = config.research.as_mut().unwrap();
    research.portfolio.accounts[0].currency = "USD".to_string().try_into().unwrap();
    research.portfolio.accounts.truncate(1);
    research.portfolio.reporting_currency = "USD".to_string().try_into().unwrap();
    research.portfolio.rates = None;
    for i in 0..2 {
        let instrument = &mut research.instruments[i];
        instrument.source_manifest = refs[i].clone();
        instrument.features.streams = Some(vec![quote]);
        instrument.features.outputs = Some(Outputs::Named(vec!["quote_delta_units".into()]));
        instrument.outcomes.expiry_seconds = vec![30];
        instrument.outcomes.max_entry_delay_ms = 1_000;
        instrument.outcomes.max_settlement_delay_ms = 1_000;
        instrument.outcomes.max_tick_gap_ms = 2_000;
        instrument.search.decision_start = start(0);
        instrument.search.decision_end = end(0);
        instrument.search.base_stream = quote;
        instrument.search.min_conditions = 2;
        instrument.search.max_conditions = 2;
        instrument.search.embargo_micros = 32_000_000;
        instrument.search.conditions = [
            (Comparator::Gt, 119.0),
            (Comparator::Lt, 2_000.0),
            (Comparator::Lt, -119.0),
            (Comparator::Gt, -2_000.0),
        ]
        .map(|(comparator, threshold)| {
            SearchCondition::Named(NamedSearchCondition {
                stream: quote,
                output: "quote_delta_units".into(),
                comparator,
                thresholds: vec![Threshold::Number(threshold)],
            })
        })
        .into();
        instrument.search.contracts =
            vec![contract(i, Direction::Sell), contract(i, Direction::Buy)];
        if i == 0 {
            instrument.search.account.currency = "USD".to_string().try_into().unwrap();
        }
        instrument.search.envelope = envelope();
        research.folds[0].inputs[i].fit_manifest = refs[i].clone();
        research.folds[0].inputs[i].assessment_manifest = refs[2 + i].clone();
        research.refit.fits[i] = refs[4 + i].clone();
        research.evaluation.inputs[i] = refs[6 + i].clone();
        research.holdout.inputs[i] = refs[8 + i].clone();
    }
    research.folds[0].cutoff = cutoff(0);
    research.folds[0].decision_start = start(1);
    research.folds[0].decision_end = end(1);
    research.refit.cutoff = cutoff(2);
    research.evaluation.decision_start = start(3);
    research.evaluation.decision_end = end(3);
    research.evaluation.splits = Some(vec![
        serde_json::from_value(serde_json::json!({"name":"a","start":start(3),"end":time(BASE + 3 * HOUR + 16 * 40_000_000)})).unwrap(),
        serde_json::from_value(serde_json::json!({"name":"b","start":time(BASE + 3 * HOUR + 16 * 40_000_000),"end":end(3)})).unwrap(),
    ]);
    research.holdout.decision_start = start(4);
    research.holdout.decision_end = end(4);
    research.holdout.splits = research.evaluation.splits.as_ref().map(|splits| {
        splits
            .iter()
            .map(|split| {
                let mut split = split.clone();
                split.start = time(
                    binary_alpha_engine::market::parse_event_time_micros(&split.start).unwrap()
                        + HOUR,
                );
                split.end = time(
                    binary_alpha_engine::market::parse_event_time_micros(&split.end).unwrap()
                        + HOUR,
                );
                split
            })
            .collect()
    });
    let portfolio = &mut research.portfolio;
    portfolio.embargo_micros = 32_000_000;
    portfolio.max_policies = 3;
    portfolio.members = vec![
        PortfolioMember {
            family: 0,
            member: 0,
            ordinals: vec![],
        },
        PortfolioMember {
            family: 0,
            member: 11,
            ordinals: vec![],
        },
    ];
    portfolio.repairs.truncate(1);
    let mut sell = portfolio.bindings[0].clone();
    sell.alternatives = vec![binary_alpha_engine::config::Alternative {
        contract: contract(0, Direction::Sell),
        envelope: envelope(),
    }];
    let mut buy = sell.clone();
    buy.id = "b1".into();
    buy.alternatives = vec![binary_alpha_engine::config::Alternative {
        contract: contract(0, Direction::Buy),
        envelope: envelope(),
    }];
    portfolio.bindings = vec![sell, buy];
    portfolio.subsets = vec![
        Subset {
            deployments: vec![Deployment {
                member: 0,
                repair: 0,
                binding: 0,
            }],
        },
        Subset {
            deployments: vec![Deployment {
                member: 1,
                repair: 0,
                binding: 1,
            }],
        },
        Subset {
            deployments: vec![
                Deployment {
                    member: 0,
                    repair: 0,
                    binding: 0,
                },
                Deployment {
                    member: 1,
                    repair: 0,
                    binding: 1,
                },
            ],
        },
    ];
    portfolio.risk_policies[0].max_open_total = Some(1);
    portfolio.risk_policies[0].max_feature_age_micros = 5_000_000;
    portfolio.risk_policies[0].max_quote_age_micros = 1_000_000;
    portfolio.risk_policies[0].max_proposal_age_micros = Some(1_000_000);
    research.scenarios = [200_000, 400_000, 600_000]
        .into_iter()
        .map(|delay| binary_alpha_engine::config::ResearchScenario {
            id: format!("delay_{}", delay / 1_000),
            acceptance_delay_micros: delay,
            alternatives: portfolio
                .bindings
                .iter()
                .map(|binding| ScenarioAlternative {
                    binding: binding.id.clone(),
                    contract: binding.alternatives[0].contract.clone(),
                    envelope: binding.alternatives[0].envelope.clone(),
                })
                .collect(),
        })
        .collect();
    (config, declaration, datasets)
}

pub fn quote_ticks(base: i64) -> Vec<String> {
    let mut lines = Vec::new();
    for cell in 0..32 {
        let start = base + cell * 40_000_000;
        let center = if cell < 16 { 100_000 } else { 100_500 };
        let direction = if cell % 2 == 0 { 1 } else { -1 };
        let mut tick = |offset, units: i64| {
            lines.push(format!(
                "{},SYNTHETIC,{}.{:05}",
                time(start + offset),
                units / 100_000,
                units % 100_000
            ));
        };
        tick(0, center);
        for (offset, distance) in [
            (100_000, 150),
            (250_000, 160),
            (450_000, 170),
            (650_000, 180),
        ] {
            tick(offset, center + direction * distance);
        }
        for second in 1..=if cell == 15 { 34 } else { 39 } {
            let distance = if second <= 30 {
                180 - 10 * second
            } else {
                -120 + 14 * (second - 30)
            };
            tick(second * 1_000_000, center + direction * distance);
        }
    }
    lines
}
