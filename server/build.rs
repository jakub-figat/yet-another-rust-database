use protobuf_codegen::Codegen;

fn main() {
    Codegen::new()
        .protoc()
        .include("src/protos")
        .inputs([
            "src/protos/request.proto",
            "src/protos/response.proto",
            "src/protos/common.proto",
        ])
        .out_dir("src/protos")
        .run_from_script();
}
