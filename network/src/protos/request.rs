// This file is generated by rust-protobuf 3.3.0. Do not edit
// .proto file is parsed by protoc 25.2
// @generated

// https://github.com/rust-lang/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(unused_attributes)]
#![cfg_attr(rustfmt, rustfmt::skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unused_results)]
#![allow(unused_mut)]

//! Generated file from `request.proto`

/// Generated files are compatible only with the same version
/// of protobuf runtime.
const _PROTOBUF_VERSION_CHECK: () = ::protobuf::VERSION_3_3_0;

// @@protoc_insertion_point(message:Request)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct Request {
    // message fields
    // @@protoc_insertion_point(field:Request.type)
    pub type_: ::protobuf::EnumOrUnknown<RequestType>,
    // @@protoc_insertion_point(field:Request.data)
    pub data: ::std::vec::Vec<u8>,
    // special fields
    // @@protoc_insertion_point(special_field:Request.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a Request {
    fn default() -> &'a Request {
        <Request as ::protobuf::Message>::default_instance()
    }
}

impl Request {
    pub fn new() -> Request {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(2);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "type",
            |m: &Request| { &m.type_ },
            |m: &mut Request| { &mut m.type_ },
        ));
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "data",
            |m: &Request| { &m.data },
            |m: &mut Request| { &mut m.data },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<Request>(
            "Request",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for Request {
    const NAME: &'static str = "Request";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.type_ = is.read_enum_or_unknown()?;
                },
                18 => {
                    self.data = is.read_bytes()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.type_ != ::protobuf::EnumOrUnknown::new(RequestType::REQUEST_TYPE_UNSPECIFIED) {
            my_size += ::protobuf::rt::int32_size(1, self.type_.value());
        }
        if !self.data.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.data);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.type_ != ::protobuf::EnumOrUnknown::new(RequestType::REQUEST_TYPE_UNSPECIFIED) {
            os.write_enum(1, ::protobuf::EnumOrUnknown::value(&self.type_))?;
        }
        if !self.data.is_empty() {
            os.write_bytes(2, &self.data)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> Request {
        Request::new()
    }

    fn clear(&mut self) {
        self.type_ = ::protobuf::EnumOrUnknown::new(RequestType::REQUEST_TYPE_UNSPECIFIED);
        self.data.clear();
        self.special_fields.clear();
    }

    fn default_instance() -> &'static Request {
        static instance: Request = Request {
            type_: ::protobuf::EnumOrUnknown::from_i32(0),
            data: ::std::vec::Vec::new(),
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for Request {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("Request").unwrap()).clone()
    }
}

impl ::std::fmt::Display for Request {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Request {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:GetRequest)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct GetRequest {
    // message fields
    // @@protoc_insertion_point(field:GetRequest.hash_key)
    pub hash_key: ::std::string::String,
    // @@protoc_insertion_point(field:GetRequest.sort_key)
    pub sort_key: ::protobuf::MessageField<super::common::Value>,
    // special fields
    // @@protoc_insertion_point(special_field:GetRequest.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a GetRequest {
    fn default() -> &'a GetRequest {
        <GetRequest as ::protobuf::Message>::default_instance()
    }
}

impl GetRequest {
    pub fn new() -> GetRequest {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(2);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "hash_key",
            |m: &GetRequest| { &m.hash_key },
            |m: &mut GetRequest| { &mut m.hash_key },
        ));
        fields.push(::protobuf::reflect::rt::v2::make_message_field_accessor::<_, super::common::Value>(
            "sort_key",
            |m: &GetRequest| { &m.sort_key },
            |m: &mut GetRequest| { &mut m.sort_key },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<GetRequest>(
            "GetRequest",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for GetRequest {
    const NAME: &'static str = "GetRequest";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                10 => {
                    self.hash_key = is.read_string()?;
                },
                18 => {
                    ::protobuf::rt::read_singular_message_into_field(is, &mut self.sort_key)?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if !self.hash_key.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.hash_key);
        }
        if let Some(v) = self.sort_key.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if !self.hash_key.is_empty() {
            os.write_string(1, &self.hash_key)?;
        }
        if let Some(v) = self.sort_key.as_ref() {
            ::protobuf::rt::write_message_field_with_cached_size(2, v, os)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> GetRequest {
        GetRequest::new()
    }

    fn clear(&mut self) {
        self.hash_key.clear();
        self.sort_key.clear();
        self.special_fields.clear();
    }

    fn default_instance() -> &'static GetRequest {
        static instance: GetRequest = GetRequest {
            hash_key: ::std::string::String::new(),
            sort_key: ::protobuf::MessageField::none(),
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for GetRequest {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("GetRequest").unwrap()).clone()
    }
}

impl ::std::fmt::Display for GetRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetRequest {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:InsertRequest)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct InsertRequest {
    // message fields
    // @@protoc_insertion_point(field:InsertRequest.hash_key)
    pub hash_key: ::std::string::String,
    // @@protoc_insertion_point(field:InsertRequest.sort_key)
    pub sort_key: ::protobuf::MessageField<super::common::Value>,
    // @@protoc_insertion_point(field:InsertRequest.values)
    pub values: ::std::vec::Vec<super::common::Value>,
    // special fields
    // @@protoc_insertion_point(special_field:InsertRequest.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a InsertRequest {
    fn default() -> &'a InsertRequest {
        <InsertRequest as ::protobuf::Message>::default_instance()
    }
}

impl InsertRequest {
    pub fn new() -> InsertRequest {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(3);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "hash_key",
            |m: &InsertRequest| { &m.hash_key },
            |m: &mut InsertRequest| { &mut m.hash_key },
        ));
        fields.push(::protobuf::reflect::rt::v2::make_message_field_accessor::<_, super::common::Value>(
            "sort_key",
            |m: &InsertRequest| { &m.sort_key },
            |m: &mut InsertRequest| { &mut m.sort_key },
        ));
        fields.push(::protobuf::reflect::rt::v2::make_vec_simpler_accessor::<_, _>(
            "values",
            |m: &InsertRequest| { &m.values },
            |m: &mut InsertRequest| { &mut m.values },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<InsertRequest>(
            "InsertRequest",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for InsertRequest {
    const NAME: &'static str = "InsertRequest";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                10 => {
                    self.hash_key = is.read_string()?;
                },
                18 => {
                    ::protobuf::rt::read_singular_message_into_field(is, &mut self.sort_key)?;
                },
                26 => {
                    self.values.push(is.read_message()?);
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if !self.hash_key.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.hash_key);
        }
        if let Some(v) = self.sort_key.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(len) + len;
        }
        for value in &self.values {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if !self.hash_key.is_empty() {
            os.write_string(1, &self.hash_key)?;
        }
        if let Some(v) = self.sort_key.as_ref() {
            ::protobuf::rt::write_message_field_with_cached_size(2, v, os)?;
        }
        for v in &self.values {
            ::protobuf::rt::write_message_field_with_cached_size(3, v, os)?;
        };
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> InsertRequest {
        InsertRequest::new()
    }

    fn clear(&mut self) {
        self.hash_key.clear();
        self.sort_key.clear();
        self.values.clear();
        self.special_fields.clear();
    }

    fn default_instance() -> &'static InsertRequest {
        static instance: InsertRequest = InsertRequest {
            hash_key: ::std::string::String::new(),
            sort_key: ::protobuf::MessageField::none(),
            values: ::std::vec::Vec::new(),
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for InsertRequest {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("InsertRequest").unwrap()).clone()
    }
}

impl ::std::fmt::Display for InsertRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for InsertRequest {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

// @@protoc_insertion_point(message:DeleteRequest)
#[derive(PartialEq,Clone,Default,Debug)]
pub struct DeleteRequest {
    // message fields
    // @@protoc_insertion_point(field:DeleteRequest.hash_key)
    pub hash_key: ::std::string::String,
    // @@protoc_insertion_point(field:DeleteRequest.sort_key)
    pub sort_key: ::protobuf::MessageField<super::common::Value>,
    // special fields
    // @@protoc_insertion_point(special_field:DeleteRequest.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a DeleteRequest {
    fn default() -> &'a DeleteRequest {
        <DeleteRequest as ::protobuf::Message>::default_instance()
    }
}

impl DeleteRequest {
    pub fn new() -> DeleteRequest {
        ::std::default::Default::default()
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(2);
        let mut oneofs = ::std::vec::Vec::with_capacity(0);
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "hash_key",
            |m: &DeleteRequest| { &m.hash_key },
            |m: &mut DeleteRequest| { &mut m.hash_key },
        ));
        fields.push(::protobuf::reflect::rt::v2::make_message_field_accessor::<_, super::common::Value>(
            "sort_key",
            |m: &DeleteRequest| { &m.sort_key },
            |m: &mut DeleteRequest| { &mut m.sort_key },
        ));
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<DeleteRequest>(
            "DeleteRequest",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for DeleteRequest {
    const NAME: &'static str = "DeleteRequest";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                10 => {
                    self.hash_key = is.read_string()?;
                },
                18 => {
                    ::protobuf::rt::read_singular_message_into_field(is, &mut self.sort_key)?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if !self.hash_key.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.hash_key);
        }
        if let Some(v) = self.sort_key.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if !self.hash_key.is_empty() {
            os.write_string(1, &self.hash_key)?;
        }
        if let Some(v) = self.sort_key.as_ref() {
            ::protobuf::rt::write_message_field_with_cached_size(2, v, os)?;
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> DeleteRequest {
        DeleteRequest::new()
    }

    fn clear(&mut self) {
        self.hash_key.clear();
        self.sort_key.clear();
        self.special_fields.clear();
    }

    fn default_instance() -> &'static DeleteRequest {
        static instance: DeleteRequest = DeleteRequest {
            hash_key: ::std::string::String::new(),
            sort_key: ::protobuf::MessageField::none(),
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for DeleteRequest {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("DeleteRequest").unwrap()).clone()
    }
}

impl ::std::fmt::Display for DeleteRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for DeleteRequest {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

#[derive(Clone,Copy,PartialEq,Eq,Debug,Hash)]
// @@protoc_insertion_point(enum:RequestType)
pub enum RequestType {
    // @@protoc_insertion_point(enum_value:RequestType.REQUEST_TYPE_UNSPECIFIED)
    REQUEST_TYPE_UNSPECIFIED = 0,
    // @@protoc_insertion_point(enum_value:RequestType.REQUEST_TYPE_GET)
    REQUEST_TYPE_GET = 1,
    // @@protoc_insertion_point(enum_value:RequestType.REQUEST_TYPE_INSERT)
    REQUEST_TYPE_INSERT = 2,
    // @@protoc_insertion_point(enum_value:RequestType.REQUEST_TYPE_DELETE)
    REQUEST_TYPE_DELETE = 3,
}

impl ::protobuf::Enum for RequestType {
    const NAME: &'static str = "RequestType";

    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<RequestType> {
        match value {
            0 => ::std::option::Option::Some(RequestType::REQUEST_TYPE_UNSPECIFIED),
            1 => ::std::option::Option::Some(RequestType::REQUEST_TYPE_GET),
            2 => ::std::option::Option::Some(RequestType::REQUEST_TYPE_INSERT),
            3 => ::std::option::Option::Some(RequestType::REQUEST_TYPE_DELETE),
            _ => ::std::option::Option::None
        }
    }

    fn from_str(str: &str) -> ::std::option::Option<RequestType> {
        match str {
            "REQUEST_TYPE_UNSPECIFIED" => ::std::option::Option::Some(RequestType::REQUEST_TYPE_UNSPECIFIED),
            "REQUEST_TYPE_GET" => ::std::option::Option::Some(RequestType::REQUEST_TYPE_GET),
            "REQUEST_TYPE_INSERT" => ::std::option::Option::Some(RequestType::REQUEST_TYPE_INSERT),
            "REQUEST_TYPE_DELETE" => ::std::option::Option::Some(RequestType::REQUEST_TYPE_DELETE),
            _ => ::std::option::Option::None
        }
    }

    const VALUES: &'static [RequestType] = &[
        RequestType::REQUEST_TYPE_UNSPECIFIED,
        RequestType::REQUEST_TYPE_GET,
        RequestType::REQUEST_TYPE_INSERT,
        RequestType::REQUEST_TYPE_DELETE,
    ];
}

impl ::protobuf::EnumFull for RequestType {
    fn enum_descriptor() -> ::protobuf::reflect::EnumDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().enum_by_package_relative_name("RequestType").unwrap()).clone()
    }

    fn descriptor(&self) -> ::protobuf::reflect::EnumValueDescriptor {
        let index = *self as usize;
        Self::enum_descriptor().value_by_index(index)
    }
}

impl ::std::default::Default for RequestType {
    fn default() -> Self {
        RequestType::REQUEST_TYPE_UNSPECIFIED
    }
}

impl RequestType {
    fn generated_enum_descriptor_data() -> ::protobuf::reflect::GeneratedEnumDescriptorData {
        ::protobuf::reflect::GeneratedEnumDescriptorData::new::<RequestType>("RequestType")
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\rrequest.proto\x1a\x0ccommon.proto\"?\n\x07Request\x12\x20\n\x04type\
    \x18\x01\x20\x01(\x0e2\x0c.RequestTypeR\x04type\x12\x12\n\x04data\x18\
    \x02\x20\x01(\x0cR\x04data\"J\n\nGetRequest\x12\x19\n\x08hash_key\x18\
    \x01\x20\x01(\tR\x07hashKey\x12!\n\x08sort_key\x18\x02\x20\x01(\x0b2\x06\
    .ValueR\x07sortKey\"m\n\rInsertRequest\x12\x19\n\x08hash_key\x18\x01\x20\
    \x01(\tR\x07hashKey\x12!\n\x08sort_key\x18\x02\x20\x01(\x0b2\x06.ValueR\
    \x07sortKey\x12\x1e\n\x06values\x18\x03\x20\x03(\x0b2\x06.ValueR\x06valu\
    es\"M\n\rDeleteRequest\x12\x19\n\x08hash_key\x18\x01\x20\x01(\tR\x07hash\
    Key\x12!\n\x08sort_key\x18\x02\x20\x01(\x0b2\x06.ValueR\x07sortKey*s\n\
    \x0bRequestType\x12\x1c\n\x18REQUEST_TYPE_UNSPECIFIED\x10\0\x12\x14\n\
    \x10REQUEST_TYPE_GET\x10\x01\x12\x17\n\x13REQUEST_TYPE_INSERT\x10\x02\
    \x12\x17\n\x13REQUEST_TYPE_DELETE\x10\x03b\x06proto3\
";

/// `FileDescriptorProto` object which was a source for this generated file
fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    static file_descriptor_proto_lazy: ::protobuf::rt::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::rt::Lazy::new();
    file_descriptor_proto_lazy.get(|| {
        ::protobuf::Message::parse_from_bytes(file_descriptor_proto_data).unwrap()
    })
}

/// `FileDescriptor` object which allows dynamic access to files
pub fn file_descriptor() -> &'static ::protobuf::reflect::FileDescriptor {
    static generated_file_descriptor_lazy: ::protobuf::rt::Lazy<::protobuf::reflect::GeneratedFileDescriptor> = ::protobuf::rt::Lazy::new();
    static file_descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::FileDescriptor> = ::protobuf::rt::Lazy::new();
    file_descriptor.get(|| {
        let generated_file_descriptor = generated_file_descriptor_lazy.get(|| {
            let mut deps = ::std::vec::Vec::with_capacity(1);
            deps.push(super::common::file_descriptor().clone());
            let mut messages = ::std::vec::Vec::with_capacity(4);
            messages.push(Request::generated_message_descriptor_data());
            messages.push(GetRequest::generated_message_descriptor_data());
            messages.push(InsertRequest::generated_message_descriptor_data());
            messages.push(DeleteRequest::generated_message_descriptor_data());
            let mut enums = ::std::vec::Vec::with_capacity(1);
            enums.push(RequestType::generated_enum_descriptor_data());
            ::protobuf::reflect::GeneratedFileDescriptor::new_generated(
                file_descriptor_proto(),
                deps,
                messages,
                enums,
            )
        });
        ::protobuf::reflect::FileDescriptor::new_generated_2(generated_file_descriptor)
    })
}
