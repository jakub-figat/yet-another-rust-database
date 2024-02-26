use protobuf_codegen::Codegen;

fn main() {
    Codegen::new()
        .protoc()
        .include("src")
        .inputs([
            "src/request.proto",
            "src/response.proto",
            "src/common.proto",
        ])
        .out_dir("src")
        .run_from_script();
}
