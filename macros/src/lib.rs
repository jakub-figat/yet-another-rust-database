use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use std::collections::BTreeMap;
use syn::punctuated::Punctuated;
use syn::token::Comma;

use storage::table::{Column, ColumnType, TableSchema};
use syn::{
    parse_macro_input, Data, DeriveInput, Field, Fields, GenericArgument, PathArguments, Type,
};

#[proc_macro_derive(DatabaseModel)]
pub fn derive_model(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident;
    let table_name = name.to_string().to_lowercase();

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("All fields of struct must be named"),
        },
        _ => panic!("Model macro can only be used with structs"),
    };

    let (sort_key, fields) = extract_fields(fields);

    let from_get_impl = proc_from_get_response(&sort_key, &fields);
    let insert_impl = proc_to_insert_request(&sort_key, &fields);
    let delete_impl = proc_to_delete_request(&sort_key);
    let table_schema_impl = proc_table_schema(&sort_key, &fields, table_name.clone());

    let expanded = quote! {
        impl Model for #name {
            fn from_get_response(get_response: GetResponse) -> Self {
                #from_get_impl
            }

            fn to_insert_request(&self) -> InsertRequest {
                #insert_impl
            }

            fn to_delete_request(&self) -> DeleteRequest {
                #delete_impl
            }

            fn hash_key(&self) -> String {
                self.hash_key.clone()
            }

            fn table_name() -> String {
                String::from(#table_name)
            }

            fn table_schema() -> TableSchema {
                #table_schema_impl
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

fn proc_from_get_response(sort_key: &Field, fields: &Vec<Field>) -> TokenStream {
    let sort_key_value_quote = parse_from_value_quote(sort_key);
    let sort_key_operation = quote! {
        let sort_key = match parse_value_from_message_field(get_response.sort_key) {
            #sort_key_value_quote
            _ => panic!("Invalid value type"),
        };
    };

    let field_operations: TokenStream = fields
        .iter()
        .map(|field| {
            let field_ident = field.ident.as_ref().unwrap();
            let field_name = field_ident.to_string();

            let value_quote = parse_from_value_quote(field);
            quote! {
                    let #field_ident = match parse_value_from_proto(
                    get_response
                    .values
                    .get(#field_name)
                    .expect(&format!("No value for field '{}'", #field_name))
                    .clone(),
                ) {
                    #value_quote
                    _ => panic!("Invalid value type"),
                };
            }
        })
        .collect();

    let struct_fields: TokenStream = fields
        .iter()
        .map(|field| {
            let field_ident = field.ident.as_ref().unwrap();
            quote! {
                #field_ident,
            }
        })
        .collect();

    quote! {
        let hash_key = get_response.hash_key.clone();
        #sort_key_operation
        #field_operations

        Self {
            hash_key,
            sort_key,
            #struct_fields
        }
    }
}

fn proc_to_insert_request(sort_key: &Field, fields: &Vec<Field>) -> TokenStream {
    let sort_key_value_quote = parse_to_value_quote(sort_key);
    let sort_key_operation = quote! {
        #sort_key_value_quote
        insert_request.sort_key = parse_message_field_from_value(value);
    };

    let field_operations: TokenStream = fields
        .into_iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap().to_string();
            let value_quote = parse_to_value_quote(field);
            quote! {
                #value_quote
                values.insert(#field_name.to_string(), parse_proto_from_value(value));
            }
        })
        .collect();

    quote! {
        let mut insert_request = InsertRequest::new();
        insert_request.hash_key = self.hash_key.clone();

        let mut values = HashMap::new();

        #sort_key_operation
        #field_operations

        insert_request.values = values;
        insert_request
    }
}

fn proc_to_delete_request(sort_key: &Field) -> TokenStream {
    let sort_key_value_quote = parse_to_value_quote(sort_key);
    let sort_key_operation = quote! {
        #sort_key_value_quote
        delete_request.sort_key = parse_message_field_from_value(value);
    };

    quote! {
        let mut delete_request = DeleteRequest::new();
        delete_request.hash_key = self.hash_key.clone();
        #sort_key_operation

        delete_request
    }
}

fn proc_table_schema(sort_key: &Field, fields: &Vec<Field>, table_name: String) -> TokenStream {
    let columns: BTreeMap<_, _> = fields
        .into_iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap().to_string();
            let (column_type, nullable) = field_to_column_type(field);
            (field_name, Column::new(column_type, nullable))
        })
        .collect();

    let (sort_key_type, nullable) = field_to_column_type(sort_key);
    if nullable {
        panic!("Sort key cannot be Option<>");
    }

    let mut table_schema = TableSchema::new(table_name, sort_key_type);
    table_schema.columns = columns;

    let table_schema_string = table_schema.to_string();
    quote! {
        TableSchema::from_string(#table_schema_string).unwrap()
    }
}

fn extract_fields(fields: &Punctuated<Field, Comma>) -> (Field, Vec<Field>) {
    let mut hash_key = None;
    let mut sort_key = None;
    let mut other_fields = Vec::new();

    for field in fields.to_owned() {
        match field.ident.as_ref().unwrap().to_string().as_str() {
            "hash_key" => hash_key = Some(field),
            "sort_key" => sort_key = Some(field),
            _ => other_fields.push(field),
        }
    }

    let hash_key = hash_key.expect("Struct must contain field 'hash_key'");
    if get_field_type(&hash_key) != "String".to_string() {
        panic!("'hash_key' must be of type 'String'");
    }

    let sort_key = sort_key.expect("Struct must contain field 'sort_key'");

    (sort_key, other_fields)
}

fn parse_from_value_quote(field: &Field) -> TokenStream {
    let field_type = get_field_type(field);
    match field_type.as_str() {
        "String" => {
            quote! {
                Varchar(val) => val,
            }
        }
        "i32" => {
            quote! {
                Int32(val) => val,
            }
        }
        "i64" => {
            quote! {
                Int64(val) => val,
            }
        }
        "u32" => {
            quote! {
                Unsigned32(val) => val,
            }
        }
        "u64" => {
            quote! {
                Unsigned64(val) => val,
            }
        }
        "f32" => {
            quote! {
                Float32(val) => val,
            }
        }
        "f64" => {
            quote! {
                Float64(val) => val,
            }
        }
        "bool" => {
            quote! {
                Boolean(val) => val,
            }
        }
        "Option" => {
            let inner_type = get_option_generic_type(field);
            let inner_type_value_quote = parse_from_value_quote_for_option(&inner_type);

            quote! {
                #inner_type_value_quote
                Null => None,
            }
        }
        // TODO
        other_type => panic!("Unsupported '{}' field type", other_type),
    }
}

fn parse_to_value_quote(field: &Field) -> TokenStream {
    let field_name = field.ident.as_ref().unwrap();
    let field_type = get_field_type(field);
    match field_type.as_str() {
        "String" => {
            quote! {
                let value = Varchar(self.#field_name.clone());
            }
        }
        "i32" => {
            quote! {
                let value = Int32(self.#field_name);
            }
        }
        "i64" => {
            quote! {
                let value = Int64(self.#field_name);
            }
        }
        "u32" => {
            quote! {
                let value = Unsigned32(self.#field_name);
            }
        }
        "u64" => {
            quote! {
                let value = Unsigned64(self.#field_name);
            }
        }
        "f32" => {
            quote! {
                let value = Float32(self.#field_name);
            }
        }
        "f64" => {
            quote! {
                let value = Float64(self.#field_name);
            }
        }
        "bool" => {
            quote! {
                let value = Boolean(self.#field_name);
            }
        }
        "Option" => {
            let inner_type = get_option_generic_type(field);
            let inner_type_value_quote = parse_to_value_quote_for_option_inner(&inner_type);

            quote! {
                let value = match &self.#field_name {
                    Some(inner) => #inner_type_value_quote
                    None => Null
                };
            }
        }
        // TODO
        other_type => panic!("Unsupported '{}' field type", other_type),
    }
}

fn get_field_type(field: &Field) -> String {
    match &field.ty {
        Type::Path(type_path) => {
            let last_segment = &type_path.path.segments.first().unwrap();
            last_segment.ident.to_string()
        }
        _ => panic!("Invalid field type"),
    }
}

fn get_option_generic_type(field: &Field) -> String {
    match &field.ty {
        Type::Path(type_path) => {
            let last_segment = &type_path.path.segments.first().unwrap();
            match last_segment.arguments {
                PathArguments::AngleBracketed(ref generics) => {
                    if generics.args.len() != 1 {
                        panic!("Invalid number of generics inside Option, should be 1");
                    }

                    match &generics.args[0] {
                        GenericArgument::Type(inner_type) => {
                            inner_type.into_token_stream().to_string()
                        }
                        _ => panic!("Invalid generic type"),
                    }
                }
                _ => panic!("Invalid Option arguments"),
            }
        }
        _ => panic!("Invalid field type"),
    }
}

fn parse_from_value_quote_for_option(field_type: &str) -> TokenStream {
    match field_type {
        "String" => {
            quote! {
                Varchar(val) => Some(val),
            }
        }
        "i32" => {
            quote! {
                Int32(val) => Some(val),
            }
        }
        "i64" => {
            quote! {
                Int64(val) => Some(val),
            }
        }
        "u32" => {
            quote! {
                Unsigned32(val) => Some(val),
            }
        }
        "u64" => {
            quote! {
                Unsigned64(val) => Some(val),
            }
        }
        "f32" => {
            quote! {
                Float32(val) => Some(val),
            }
        }
        "f64" => {
            quote! {
                Float64(val) => Some(val),
            }
        }
        "bool" => {
            quote! {
                Boolean(val) => Some(val),
            }
        }
        // TODO
        other_type => panic!("Unsupported '{}' field type", other_type),
    }
}

fn parse_to_value_quote_for_option_inner(field_type: &str) -> TokenStream {
    match field_type {
        "String" => {
            quote! {
                Varchar(inner.clone()),
            }
        }
        "i32" => {
            quote! {
                Int32(inner.clone()),
            }
        }
        "i64" => {
            quote! {
                Int64(inner.clone()),
            }
        }
        "u32" => {
            quote! {
                Unsigned32(inner.clone()),
            }
        }
        "u64" => {
            quote! {
                Unsigned64(inner.clone()),
            }
        }
        "f32" => {
            quote! {
                Float32(inner.clone()),
            }
        }
        "f64" => {
            quote! {
                Float64(inner.clone()),
            }
        }
        "bool" => {
            quote! {
                Boolean(inner.clone()),
            }
        }
        other_type => panic!("Unsupported '{}' field type", other_type),
    }
}

fn field_to_column_type(field: &Field) -> (ColumnType, bool) {
    let field_type = get_field_type(field);
    match field_type.as_str() {
        "String" => (ColumnType::Varchar(1024), false),
        "i32" => (ColumnType::Int32, false),
        "i64" => (ColumnType::Int64, false),
        "u32" => (ColumnType::Unsigned32, false),
        "u64" => (ColumnType::Unsigned64, false),
        "f32" => (ColumnType::Float32, false),
        "f64" => (ColumnType::Float64, false),
        "Boolean" => (ColumnType::Boolean, false),
        "Option" => {
            let generic_type = get_option_generic_type(field);
            (option_generic_type_to_column_type(&generic_type), true)
        }
        other_type => panic!("Unsupported '{}' field type", other_type),
    }
}

fn option_generic_type_to_column_type(field_type: &str) -> ColumnType {
    match field_type {
        "String" => ColumnType::Varchar(1000),
        "i32" => ColumnType::Int32,
        "i64" => ColumnType::Int64,
        "u32" => ColumnType::Unsigned32,
        "u64" => ColumnType::Unsigned64,
        "f32" => ColumnType::Float32,
        "f64" => ColumnType::Float64,
        "Boolean" => ColumnType::Boolean,
        other_type => panic!("Unsupported '{}' field type", other_type),
    }
}
