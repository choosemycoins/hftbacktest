use std::{fs::File,io::{BufRead,BufReader,BufWriter,Write}};
use flate2::read::MultiGzDecoder;
use serde_json::Value;

#[derive(Debug,PartialEq)]
struct Row { ints:[i64;5], floats:[f64;4] }
fn parse(line:&str)->Result<Option<Row>,Box<dyn std::error::Error>> {
    if !line.contains("\"e\":\"trade\"") && !line.contains("\"e\":\"bookTicker\"") && !line.contains("\"nextFundingTime\"") { return Ok(None); }
    let (stamp,raw)=line.split_once(' ').ok_or("missing timestamp")?;
    let local:i64=stamp.parse()?;
    let v:Value=serde_json::from_str(raw)?;let d=v.get("data").unwrap_or(&v);
    let num=|key:&str|->Result<f64,Box<dyn std::error::Error>> {Ok(d[key].as_str().ok_or("missing decimal")?.parse()?)};
    let integer=|key:&str|->Result<i64,Box<dyn std::error::Error>> {Ok(d[key].as_i64().ok_or("missing integer")?)};
    let r=match d["e"].as_str() {
      Some("trade")=> {
        if d["X"].as_str().ok_or("missing trade type")?!="MARKET" {return Ok(None);}
        Row{ints:[local,integer("T")?*1_000_000,2,integer("t")?,if d["m"].as_bool().ok_or("missing side")? {-1}else{1}],floats:[num("p")?,num("q")?,0.,0.]}
      },
      Some("bookTicker")=>Row{ints:[local,integer("T")?*1_000_000,1,integer("u")?,0],floats:[num("b")?,num("B")?,num("a")?,num("A")?]},
      None if d.get("nextFundingTime").is_some()=>Row{ints:[local,integer("time")?*1_000_000,3,integer("nextFundingTime")?*1_000_000,0],floats:[num("markPrice")?,num("lastFundingRate")?,0.,0.]},
      _=>return Ok(None),
    };
    if r.floats.iter().any(|x|!x.is_finite()) {return Err("non-finite decimal".into());}
    Ok(Some(r))
}

fn main()->Result<(),Box<dyn std::error::Error>> {
    let a:Vec<String>=std::env::args().collect();
    let mut input=BufReader::new(MultiGzDecoder::new(File::open(&a[1])?));
    let mut output=BufWriter::new(File::create(&a[2])?);
    let mut line=String::new();let mut counts=[0u64;4];
    while input.read_line(&mut line)?>0 {
        if let Some(r)=parse(&line)? {
            counts[r.ints[2] as usize]+=1;
            for v in r.ints { output.write_all(&v.to_le_bytes())?; }
            for v in r.floats { output.write_all(&v.to_le_bytes())?; }
        }
        line.clear();
    }
    output.flush()?;println!("{}",serde_json::json!({"book":counts[1],"trade":counts[2],"premium":counts[3]}));Ok(())
}

#[cfg(test)]
mod tests {
 use super::*;
 #[test] fn trade_side_is_aggressor_and_timestamps_keep_nanoseconds() {
  let r=parse(r#"1786492803277153484 {"data":{"e":"trade","T":1786492803275,"t":7,"p":"0.6","q":"12","X":"MARKET","m":true}}"#).unwrap().unwrap();
  assert_eq!(r.ints,[1786492803277153484,1786492803275000000,2,7,-1]);
  assert_eq!(r.floats,[0.6,12.,0.,0.]);
 }
 #[test] fn records_book_size_and_next_funding_not_as_trades() {
  let b=parse(r#"10000000001 {"data":{"e":"bookTicker","T":10000,"u":9,"b":"99","B":"3","a":"101","A":"4"}}"#).unwrap().unwrap();
  assert_eq!(b.ints[2],1);assert_eq!(b.floats,[99.,3.,101.,4.]);
  let f=parse(r#"10000000001 {"symbol":"X","markPrice":"100","lastFundingRate":"0.001","nextFundingTime":20000,"time":10000}"#).unwrap().unwrap();
  assert_eq!(f.ints[2],3);assert_eq!(f.ints[3],20000000000);
 }
 #[test] fn rejects_broken_relevant_json_and_excludes_nonmarket_trades() {
  assert!(parse(r#"10 {"data":{"e":"trade"}}"#).is_err());
  assert!(parse(r#"10 {"data":{"e":"trade","X":"INSURANCE_FUND"}}"#).unwrap().is_none());
  assert!(parse(r#"10 {"data":{"e":"depthUpdate"}}"#).unwrap().is_none());
 }
}
