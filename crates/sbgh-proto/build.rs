use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let workspace = manifest_dir
        .parent()
        .and_then(std::path::Path::parent)
        .ok_or("sbgh-proto must live below the workspace root")?;
    let proto_root = workspace.join("proto");
    let schema = proto_root.join("sbgh/fleet/v1/fleet.proto");
    let protoc = protoc_bin_vendored::protoc_bin_path()?;

    let mut prost = prost_build::Config::new();
    prost
        .protoc_executable(protoc)
        .skip_debug([".sbgh.fleet.v1"]);

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .emit_rerun_if_changed(true)
        .compile_with_config(prost, &[schema], &[proto_root])?;

    Ok(())
}
