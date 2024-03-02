use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::punctuated::Punctuated;
use syn::token::Comma;

use syn::{parse_macro_input, Data, DeriveInput, Field, Fields, Type};

#[proc_macro_derive(Model)]
pub fn derive_model(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident;
    let mut table_name = name.to_string().to_lowercase();
    table_name.push('s');

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("All fields of struct must be named"),
        },
        _ => panic!("Model macro can only be used with structs"),
    };

    if fields
        .iter()
        .find(|field| {
            field.ident.as_ref().unwrap().to_string() == "hash_key".to_string()
                && get_field_type(field) == "String".to_string()
        })
        .is_none()
    {
        panic!("Struct must contain field 'hash_key' with String type");
    }

    if fields
        .iter()
        .find(|field| field.ident.as_ref().unwrap().to_string() == "sort_key".to_string())
        .is_none()
    {
        panic!("Struct must contain field 'sort_key'");
    }

    let insert_impl = proc_to_insert_request(&fields, table_name);
    let expanded = quote! {
        impl Model for #name {
            fn to_insert_request(&self) -> InsertRequest {
                #insert_impl
            }
        }
    };
    // let expanded = quote! {
    //     impl Model for #name {
    //         fn to_insert_request(&self) -> InsertRequest {
    //             InsertRequest::new()
    //         }
    //     }
    // };

    proc_macro::TokenStream::from(expanded)
}

fn proc_to_insert_request(fields: &Punctuated<Field, Comma>, table_name: String) -> TokenStream {
    let field_operations: TokenStream = fields
        .into_iter()
        .filter(|field| field.ident.as_ref().unwrap().to_string() != "hash_key".to_string())
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();

            let field_type = get_field_type(field);
            let value_quote = match field_type.as_str() {
                "String" => {
                    quote! {
                        let value = Varchar(self.#field_name.clone(), 1);
                    }
                }
                // TODO
                other_type => panic!("Unsupported '{}' field type", other_type),
            };

            if field.ident.as_ref().unwrap().to_string() == "sort_key".to_string() {
                quote! {
                    #value_quote
                    insert_request.sort_key = parse_message_field_from_value(value);
                }
            } else {
                quote! {
                    #value_quote
                    values.push(parse_proto_from_value(value));
                }
            }
        })
        .collect();

    quote! {
        let mut insert_request = InsertRequest::new();
        insert_request.hash_key = self.hash_key.clone();

        let mut values = Vec::new();

        #field_operations

        insert_request.values = values;
        insert_request.table = String::from(#table_name);
        insert_request
    }
}

//
// impl Model for User {
//     fn to_insert_request(&self) -> InsertRequest {
//         let mut insert_request = InsertRequest::new();
//         insert_request.hash_key = self.hash_key.clone();
//         insert_request.sort_key = parse_message_field_from_value(Unsigned32(self.sort_key));
//
//         let mut values = Vec::new();
//         values.push(parse_proto_from_value(Varchar(self.username.clone(), 1)));
//         values.push(parse_proto_from_value(Unsigned32(self.age)));
//         insert_request.values = values;
//
//         insert_request.table = "users".to_string();
//         insert_request
//     }
// }

fn get_field_type(field: &Field) -> String {
    match &field.ty {
        Type::Path(type_path) => type_path.into_token_stream().to_string(),
        _ => panic!("Invalid field type"),
    }
}
