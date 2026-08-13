//! Guards PR 5.7's workspace release-profile decision against silent drift.

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");

#[test]
fn workspace_release_profile_keeps_thin_lto() {
    assert_eq!(release_lto(WORKSPACE_MANIFEST), Ok("thin"));
}

#[test]
fn lto_policy_rejects_disabled_missing_and_comment_only_values() {
    let cases = [
        ("[workspace]\nmembers = []\n", Err("missing [profile.release]")),
        ("[profile.release]\nopt-level = 3\n", Err("missing lto")),
        ("[profile.release]\nlto = false\n", Err("lto disabled")),
        ("[profile.release]\nlto = \"off\"\n", Err("lto disabled")),
        (
            "[profile.release]\n# lto = \"thin\"\ncodegen-units = 16\n",
            Err("missing lto"),
        ),
    ];

    for (manifest, expected) in cases {
        assert_eq!(release_lto(manifest), expected, "manifest:\n{manifest}");
    }
}

#[test]
fn profile_comment_does_not_declare_codegen_units() {
    let release = table_body(WORKSPACE_MANIFEST, "[profile.release]").expect("release profile");
    assert_eq!(assignment_value(release, "codegen-units"), None);
}
