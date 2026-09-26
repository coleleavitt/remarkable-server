//! Minimal namespace-aware XML element tree for WebDAV multistatus responses.

use quick_xml::NsReader;
use quick_xml::events::Event;
use quick_xml::name::{Namespace, ResolveResult};

pub(super) const DAV: &str = "DAV:";
pub(super) const CALDAV: &str = "urn:ietf:params:xml:ns:caldav";

/// Nesting deeper than any WebDAV response needs is rejected rather than parsed.
const MAX_DEPTH: usize = 64;
/// Elements plus attributes kept per document. Each one costs well over a hundred bytes in
/// the tree for as few as four bytes of body (`<a/>`), so the response size cap alone would
/// still let a server make the tree some 40 times larger than the body (1.3 GB from 32 MiB).
/// A `calendar-query` REPORT answer takes about seven elements per returned event, so this
/// still admits some 70,000 events in one answer, at under 100 MB of tree.
const MAX_NODES: usize = 500_000;

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

/// Take one node from `budget`, failing once it is spent.
fn take_node(budget: &mut usize, limit: usize) -> Result<(), String> {
    *budget = budget.checked_sub(1).ok_or_else(|| {
        format!(
            "XML document has more than {} elements and attributes",
            limit
        )
    })?;
    Ok(())
}

/// The element `e` opens, its attributes included, charged to `budget` as it is built.
fn element(
    ns: String,
    e: &quick_xml::events::BytesStart<'_>,
    budget: &mut usize,
    limit: usize,
) -> Result<Element, String> {
    take_node(budget, limit)?;
    let mut attrs = Vec::new();
    for a in e.attributes().flatten() {
        take_node(budget, limit)?;
        attrs.push((
            String::from_utf8_lossy(a.key.local_name().as_ref()).into_owned(),
            String::from_utf8_lossy(&a.value).into_owned(),
        ));
    }
    Ok(Element {
        ns,
        name: String::from_utf8_lossy(e.local_name().as_ref()).into_owned(),
        attrs,
        ..Default::default()
    })
}

/// A well-formed document has exactly one root element; anything after it is a broken or
/// concatenated response, not more of the answer.
const SECOND_ROOT: &str = "XML document has more than one root element";

/// Parse a document into its root element.
pub(super) fn parse(xml: &str) -> Result<Element, String> {
    parse_limited(xml, MAX_NODES)
}

/// [`parse`], failing once the document has more than `max_nodes` elements and attributes.
fn parse_limited(xml: &str, max_nodes: usize) -> Result<Element, String> {
    let mut reader = NsReader::from_str(xml);
    let mut stack: Vec<Element> = Vec::new();
    let mut root: Option<Element> = None;
    let mut budget = max_nodes;
    loop {
        let (res, event) = reader.read_resolved_event().map_err(|e| e.to_string())?;
        match event {
            Event::Start(e) => {
                if stack.len() >= MAX_DEPTH {
                    return Err("XML nested too deeply".into());
                }
                if stack.is_empty() && root.is_some() {
                    return Err(SECOND_ROOT.into());
                }
                stack.push(element(namespace(&res), &e, &mut budget, max_nodes)?);
            }
            Event::Empty(e) => {
                if stack.is_empty() && root.is_some() {
                    return Err(SECOND_ROOT.into());
                }
                let el = element(namespace(&res), &e, &mut budget, max_nodes)?;
                match stack.last_mut() {
                    Some(parent) => parent.children.push(el),
                    None => root = Some(el),
                }
            }
            Event::End(_) => {
                let el = stack.pop().ok_or("unbalanced end tag")?;
                match stack.last_mut() {
                    Some(parent) => parent.children.push(el),
                    None => root = Some(el),
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
        for two_roots in ["<a/><b/>", "<a></a><b>x</b>", "<a/><b></b>", "<a></a><b/>"] {
            assert_eq!(parse(two_roots).unwrap_err(), SECOND_ROOT, "{}", two_roots);
        }
        // Comments, processing instructions and whitespace around the root are fine.
        assert!(parse("<?xml version=\"1.0\"?>\n<a/>\n<!-- done -->\n").is_ok());
    }

    #[test]
    fn counts_elements_and_attributes_against_the_limit() {
        // Three elements with one attribute each (the root's is its namespace declaration).
        let doc = r#"<d:m xmlns:d="DAV:"><d:a x="1"/><d:b y="2">t</d:b></d:m>"#;
        assert_eq!(parse_limited(doc, 6).unwrap().children.len(), 2);
        let err = parse_limited(doc, 5).unwrap_err();
        assert!(
            err.contains("more than 5 elements and attributes"),
            "{}",
            err
        );
        // Attributes alone exhaust it too, before they are all collected.
        let many_attrs = format!(
            "<a {}/>",
            (0..10)
                .map(|i| format!("k{}=''", i))
                .collect::<Vec<_>>()
                .join(" ")
        );
        assert!(parse_limited(&many_attrs, 11).is_ok());
        assert!(parse_limited(&many_attrs, 10).is_err());
    }

    #[test]
    fn rejects_a_flood_of_elements_at_the_real_limit() {
        let flood = |n: usize| {
            format!(
                "<d:multistatus xmlns:d=\"DAV:\">{}</d:multistatus>",
                "<a/>".repeat(n)
            )
        };
        // Root and its namespace declaration are two of the nodes.
        assert!(parse(&flood(MAX_NODES - 2)).is_ok());
        let err = parse(&flood(MAX_NODES - 1)).unwrap_err();
        assert!(err.contains("elements and attributes"), "{}", err);
    }

    proptest::proptest! {
        #[test]
        fn never_panics(s in ".{0,400}") {
            let _ = parse(&s);
        }
    }
}
