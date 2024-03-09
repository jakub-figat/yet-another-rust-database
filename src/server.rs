use server::run_listener_threads;
use std::thread::available_parallelism;

fn main() {
    let num_of_threads = available_parallelism().unwrap().get();
    run_listener_threads(num_of_threads);
    // let mut runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
    //     .enable_timer()
    //     .build()
    //     .unwrap();

    // TODO: for now manual table schema
    // runtime.block_on(async {
    //     let table_schemas = vec![
    //         TableSchema {
    //             name: "user".to_string(),
    //             columns: BTreeMap::from([
    //                 (
    //                     "name".to_string(),
    //                     Column {
    //                         column_type: ColumnType::Varchar(100),
    //                         nullable: false,
    //                     },
    //                 ),
    //                 (
    //                     "age".to_string(),
    //                     Column {
    //                         column_type: ColumnType::Unsigned32,
    //                         nullable: true,
    //                     },
    //                 ),
    //             ]),
    //         },
    //         TableSchema {
    //             name: "post".to_string(),
    //             columns: BTreeMap::from([
    //                 (
    //                     "title".to_string(),
    //                     Column {
    //                         column_type: ColumnType::Varchar(100),
    //                         nullable: false,
    //                     },
    //                 ),
    //                 (
    //                     "is_published".to_string(),
    //                     Column {
    //                         column_type: ColumnType::Boolean,
    //                         nullable: false,
    //                     },
    //                 ),
    //                 (
    //                     "rating".to_string(),
    //                     Column {
    //                         column_type: ColumnType::Decimal(3, 2),
    //                         nullable: false,
    //                     },
    //                 ),
    //             ]),
    //         },
    //     ];

    // write_table_schemas_to_file(table_schemas).await.unwrap();
    // println!("{:?}", read_table_schemas().await.unwrap())
    // });
}

// skiplist expected times
// 6secs for 1mln, 42s! for 5mln, 2mins! for 10mln
// with unsafe: 3s for 1mln, 21s for 5mln, 53s for 10mln

// on release build: 1sec for 1mln, 10secs for 5mln, 28s for 10mln
// on release with unsafe: 0.8s for 1mln, 9sec for 5mln, 24s for 10mln
