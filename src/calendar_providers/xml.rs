//! Minimal namespace-aware XML element tree for WebDAV multistatus responses.

use quick_xml::NsReader;
use quick_xml::events::Event;
use quick_xml::name::{Namespace, ResolveResult};

pub(super) const DAV: &str = "DAV:";
pub(super) const CALDAV: &str = "urn:ietf:params:xml:ns:caldav";

/// Nesting deeper than any WebDAV response needs is rejected rather than parsed.
const MAX_DEPTH: usize = 64;

#[derive(Debug, Default)]
pub(super) struct Element {
    /// Namespace URI; empty when unbound.
    pub ns: String,
    pub name: String,
    pub attrs: Vec<(String, String)>,
    /// Concatenated character data (text, CDATA and resolved references).
    pub text: String,
    pub children: Vec<Element>,
}

impl Element {
    pub fn is(&self, ns: &str, name: &str) -> bool {
        self.ns == ns && self.name == name
    }

    pub fn child(&self, ns: &str, name: &str) -> Option<&Element> {
        self.children.iter().find(|c| c.is(ns, name))
    }

    pub fn children_named<'a>(
        &'a self,
        ns: &'a str,
        name: &'a str,
    ) -> impl Iterator<Item = &'a Element> + 'a {
        self.children.iter().filter(move |c| c.is(ns, name))
    }

    pub fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

fn namespace(res: &ResolveResult<'_>) -> String {
    match res {
        ResolveResult::Bound(Namespace(ns)) => String::from_utf8_lossy(ns).into_owned(),
        _ => String::new(),
    }
}

fn element(ns: String, e: &quick_xml::events::BytesStart<'_>) -> Element {
    let attrs = e
        .attributes()
        .flatten()
        .map(|a| {
            (
                String::from_utf8_lossy(a.key.local_name().as_ref()).into_owned(),
                String::from_utf8_lossy(&a.value).into_owned(),
            )
        })
        .collect();
    Element {
        ns,
        name: String::from_utf8_lossy(e.local_name().as_ref()).into_owned(),
        attrs,
        ..Default::default()
    }
}

/// Parse a document into its root element.
pub(super) fn parse(xml: &str) -> Result<Element, String> {
    let mut reader = NsReader::from_str(xml);
    let mut stack: Vec<Element> = Vec::new();
    let mut root: Option<Element> = None;
    loop {
        let (res, event) = reader.read_resolved_event().map_err(|e| e.to_string())?;
        match event {
            Event::Start(e) => {
                if stack.len() >= MAX_DEPTH {
                    return Err("XML nested too deeply".into());
                }
                stack.push(element(namespace(&res), &e));
            }
            Event::Empty(e) => {
                let el = element(namespace(&res), &e);
                match stack.last_mut() {
                    Some(parent) => parent.children.push(el),
                    None => root = root.or(Some(el)),
                }
            }
            Event::End(_) => {
                let el = stack.pop().ok_or("unbalanced end tag")?;
                match stack.last_mut() {
                    Some(parent) => parent.children.push(el),
                    None => root = root.or(Some(el)),
                }
            }
            Event::Text(t) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&t.decode().map_err(|e| e.to_string())?);
                }
            }
            Event::CData(t) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&t.decode().map_err(|e| e.to_string())?);
                }
            }
            Event::GeneralRef(r) => {
                if let Some(top) = stack.last_mut() {
                    if let Some(c) = r.resolve_char_ref().map_err(|e| e.to_string())? {
                        top.text.push(c);
                    } else {
                        let name = r.decode().map_err(|e| e.to_string())?;
                        match quick_xml::escape::resolve_predefined_entity(&name) {
                            Some(v) => top.text.push_str(v),
                            None => return Err(format!("unknown entity &{};", name)),
                        }
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    if !stack.is_empty() {
        return Err("unexpected end of document".into());
    }
    root.ok_or_else(|| "no root element".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_namespaces_prefixes_and_references() {
        let doc = r#"<?xml version="1.0"?>
<multistatus xmlns="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <response><href>/a%20b/</href>
    <propstat><prop><c:calendar-data>A&amp;B&#13;&#x41;<![CDATA[<x>]]></c:calendar-data>
    <c:comp name="VEVENT"/></prop></propstat>
  </response>
</multistatus>"#;
        let root = parse(doc).unwrap();
        assert!(root.is(DAV, "multistatus"));
        let prop = root
            .child(DAV, "response")
            .and_then(|r| r.child(DAV, "propstat"))
            .and_then(|p| p.child(DAV, "prop"))
            .unwrap();
        assert_eq!(
            prop.child(CALDAV, "calendar-data").unwrap().text,
            "A&B\rA<x>"
        );
        assert_eq!(
            prop.child(CALDAV, "comp").unwrap().attr("name"),
            Some("VEVENT")
        );
    }

    #[test]
    fn rejects_malformed_documents() {
        assert!(parse("<a><b></a>").is_err());
        assert!(parse("<a>").is_err());
        assert!(parse("").is_err());
        assert!(parse("<a>&bogus;</a>").is_err());
    }

    proptest::proptest! {
        #[test]
        fn never_panics(s in ".{0,400}") {
            let _ = parse(&s);
        }
    }
}
