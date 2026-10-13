use clickhouse_promql::{engine::Engine,histogram::Histogram,model::{Labels,Point,Series}};
use std::collections::BTreeMap;
fn input()->Vec<Series>{vec![Series{labels:Labels::from([("__name__".into(),"latency".into())]),points:vec![Point{timestamp:0,histogram:Some(Histogram{schema:0,count:4.0,sum:6.0,positive:BTreeMap::from([(1,4.0)]),..Default::default()}),..Default::default()}]}]}
#[test]fn native_quantile_uses_exponential_interpolation(){let data=input();let result=Engine::new(&data,0,0).instant("histogram_quantile(0.5,latency)",0).unwrap().vector().unwrap();assert!((result[0].value-2.0f64.sqrt()).abs()<1e-12);}
#[test]fn native_json_and_scalar_multiplication(){let data=input();let result=Engine::new(&data,0,0).instant("latency * 2",0).unwrap();insta::assert_json_snapshot!("native_histogram",result.json(0));}
#[test]fn native_fraction_interpolation(){let data=input();let result=Engine::new(&data,0,0).instant("histogram_fraction(1,sqrt(2),latency)",0);assert!(result.is_err());let result=Engine::new(&data,0,0).instant("histogram_fraction(1,1.4142135623730951,latency)",0).unwrap().vector().unwrap();assert!((result[0].value-0.5).abs()<1e-12);}
#[test]fn float_functions_ignore_native_samples(){let data=input();assert!(Engine::new(&data,0,0).instant("abs(latency)",0).unwrap().vector().unwrap().is_empty());}
