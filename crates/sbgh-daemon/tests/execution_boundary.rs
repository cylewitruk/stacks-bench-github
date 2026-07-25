//! Architecture ratchet for the in-process execution closure.
//!
//! Starting from the task dispatcher and every production `Driver`
//! implementation, this test follows production `mod` declarations and local
//! module references. A new backend or imported local module therefore joins
//! the checked closure automatically.

use std::collections::{BTreeSet, VecDeque};
use std::path::{Path, PathBuf};

use syn::visit::{self, Visit};
use syn::{Attribute, Item, ItemUse, UseTree};

const DISPATCH_ROOT: &str = "execution.rs";

#[derive(Default)]
struct ProductionReferences {
    implements_driver: bool,
    module_declarations: BTreeSet<String>,
    paths: Vec<Vec<String>>,
}

impl<'ast> Visit<'ast> for ProductionReferences {
    fn visit_item(&mut self, item: &'ast Item) {
        if is_test_only(item_attributes(item)) {
            return;
        }
        match item {
            Item::ExternCrate(external) => {
                self.paths
                    .push(vec![external.ident.to_string()]);
            }
            Item::Impl(implementation)
                if implementation
                    .trait_
                    .as_ref()
                    .is_some_and(|(_, path, _)| {
                        path.segments
                            .last()
                            .is_some_and(|segment| segment.ident == "Driver")
                    }) =>
            {
                self.implements_driver = true;
            }
            Item::Mod(module) if module.content.is_none() => {
                self.module_declarations
                    .insert(module.ident.to_string());
            }
            Item::Use(item_use) => collect_use_paths(item_use, &mut self.paths),
            _ => {}
        }
        visit::visit_item(self, item);
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        self.paths.push(
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect(),
        );
        visit::visit_path(self, path);
    }
}

fn item_attributes(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        _ => &[],
    }
}

fn is_test_only(attributes: &[Attribute]) -> bool {
    attributes
        .iter()
        .any(|attribute| {
            if !attribute
                .path()
                .is_ident("cfg")
            {
                return false;
            }
            let syn::Meta::List(arguments) = &attribute.meta else {
                return false;
            };
            arguments
                .tokens
                .to_string()
                .replace(' ', "")
                == "test"
        })
}

fn collect_use_paths(item: &ItemUse, paths: &mut Vec<Vec<String>>) {
    fn walk(tree: &UseTree, prefix: &mut Vec<String>, paths: &mut Vec<Vec<String>>) {
        match tree {
            UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                walk(&path.tree, prefix, paths);
                prefix.pop();
            }
            UseTree::Name(name) => {
                prefix.push(name.ident.to_string());
                paths.push(prefix.clone());
                prefix.pop();
            }
            UseTree::Rename(rename) => {
                prefix.push(rename.ident.to_string());
                paths.push(prefix.clone());
                prefix.pop();
            }
            UseTree::Glob(_) => paths.push(prefix.clone()),
            UseTree::Group(group) => {
                for tree in &group.items {
                    walk(tree, prefix, paths);
                }
            }
        }
    }

    walk(&item.tree, &mut Vec::new(), paths);
}

fn production_references(source: &str) -> ProductionReferences {
    let file = syn::parse_file(source).expect("production Rust source must parse");
    let mut references = ProductionReferences::default();
    for item in &file.items {
        references.visit_item(item);
    }
    references
}

fn module_directory(source: &Path) -> PathBuf {
    if source
        .file_name()
        .is_some_and(|name| name == "mod.rs")
    {
        source
            .parent()
            .unwrap()
            .to_path_buf()
    } else {
        source
            .parent()
            .unwrap()
            .join(source.file_stem().unwrap())
    }
}

fn resolve_module(directory: &Path, name: &str) -> Option<PathBuf> {
    let file = directory.join(format!("{name}.rs"));
    if file.is_file() {
        return Some(file);
    }
    let module = directory
        .join(name)
        .join("mod.rs");
    module
        .is_file()
        .then_some(module)
}

fn parent_module_directory(source: &Path) -> PathBuf {
    if source
        .file_name()
        .is_some_and(|name| name == "mod.rs")
    {
        source
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf()
    } else {
        source
            .parent()
            .unwrap()
            .to_path_buf()
    }
}

fn rust_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_sources(&path, sources);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "rs")
            && !path
                .file_name()
                .is_some_and(|name| {
                    name == "tests.rs"
                        || name == "test_support.rs"
                        || name
                            .to_string_lossy()
                            .ends_with("_tests.rs")
                })
        {
            sources.push(path);
        }
    }
}

fn driver_roots(source_root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    rust_sources(source_root, &mut sources);
    sources
        .into_iter()
        .filter(|source| {
            let text = std::fs::read_to_string(source).unwrap();
            production_references(&text).implements_driver
        })
        .collect()
}

fn discover_closure(source_root: &Path) -> BTreeSet<PathBuf> {
    let mut pending = VecDeque::from([source_root.join(DISPATCH_ROOT)]);
    let drivers = driver_roots(source_root);
    assert!(
        !drivers.is_empty(),
        "execution closure must contain at least one production Driver implementation"
    );
    pending.extend(drivers);
    let mut closure = BTreeSet::new();

    while let Some(source) = pending.pop_front() {
        let relative = source
            .strip_prefix(source_root)
            .unwrap()
            .to_path_buf();
        if !closure.insert(relative) {
            continue;
        }

        let text = std::fs::read_to_string(&source)
            .unwrap_or_else(|error| panic!("reading {}: {error}", source.display()));
        let references = production_references(&text);
        let directory = module_directory(&source);
        for module in references.module_declarations {
            let child = resolve_module(&directory, &module).unwrap_or_else(|| {
                panic!(
                    "{} declares module {module:?}, but no source file resolves",
                    source.display()
                )
            });
            pending.push_back(child);
        }
        for path in references.paths {
            if let Some(module) = path.get(1) {
                let directory = match path
                    .first()
                    .map(String::as_str)
                {
                    Some("crate") => Some(source_root.to_path_buf()),
                    Some("super") => Some(parent_module_directory(&source)),
                    Some("self") => Some(module_directory(&source)),
                    _ => None,
                };
                if let Some(child) =
                    directory.and_then(|directory| resolve_module(&directory, module))
                {
                    pending.push_back(child);
                }
            }
        }
    }

    closure
}

fn forbidden_dependency(path: &[String]) -> Option<&'static str> {
    if path
        .iter()
        .any(|segment| matches!(segment.as_str(), "DaemonConfig" | "RunnableJob" | "Prepared"))
    {
        return Some("aggregate orchestrator type");
    }
    if path
        .first()
        .is_some_and(|root| matches!(root.as_str(), "octocrab" | "sqlx"))
    {
        return Some("orchestrator runtime client");
    }
    if path
        .first()
        .is_some_and(|root| root == "sbgh_core")
        && path
            .get(1)
            .is_some_and(|module| matches!(module.as_str(), "db" | "github" | "models"))
    {
        return Some("orchestrator core module");
    }
    if path
        .first()
        .is_some_and(|root| root == "crate")
        && path
            .get(1)
            .is_some_and(|module| {
                matches!(module.as_str(), "job_source" | "report" | "reporter" | "runner" | "slack")
            })
    {
        return Some("orchestrator daemon module");
    }
    None
}

#[test]
fn execution_dependency_closure_is_derived_and_orchestrator_free() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let closure = discover_closure(&source_root);

    for required in [
        "artifact_store.rs",
        "bench_recipe.rs",
        "binary_cache.rs",
        "driver.rs",
        "libvirt/driver.rs",
        "recipe.rs",
    ] {
        assert!(
            closure.contains(Path::new(required)),
            "derived execution closure omitted required module {required}; closure: {closure:#?}"
        );
    }

    let mut violations = Vec::new();
    for relative in &closure {
        let source = std::fs::read_to_string(source_root.join(relative)).unwrap();
        for path in production_references(&source).paths {
            if let Some(reason) = forbidden_dependency(&path) {
                violations.push(format!("{}: {} ({reason})", relative.display(), path.join("::")));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "execution closure contains forbidden dependencies:\n{}",
        violations.join("\n")
    );
}

#[test]
fn cfg_test_item_does_not_hide_later_production_dependencies() {
    let references = production_references(
        r#"
            use crate::driver::Driver;

            #[cfg(test)]
            mod tests {
                use octocrab::Octocrab;
            }

            use sqlx::Pool;
        "#,
    );

    assert!(
        references
            .paths
            .iter()
            .any(|path| path == &["sqlx", "Pool"]),
        "production dependency after cfg(test) must remain visible"
    );
    assert!(
        !references
            .paths
            .iter()
            .any(|path| path
                .first()
                .is_some_and(|root| root == "octocrab")),
        "test-only dependency must be excluded"
    );
}

#[test]
fn newly_imported_local_module_joins_the_closure_automatically() {
    let temp = tempfile::tempdir().unwrap();
    let source_root = temp.path().join("src");
    std::fs::create_dir_all(&source_root).unwrap();
    std::fs::write(source_root.join("execution.rs"), "").unwrap();
    std::fs::write(
        source_root.join("new_backend.rs"),
        "use crate::new_dependency::ExecutionHelper;\nimpl Driver for NewBackend {}",
    )
    .unwrap();
    std::fs::write(source_root.join("new_dependency.rs"), "pub struct ExecutionHelper;").unwrap();

    let closure = discover_closure(&source_root);
    assert!(closure.contains(Path::new("new_backend.rs")));
    assert!(closure.contains(Path::new("new_dependency.rs")));
}
