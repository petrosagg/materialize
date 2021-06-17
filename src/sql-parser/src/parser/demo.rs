use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Connector {
    File(String, FileOptions),
    S3(S3Options),
    Postgres(PostgresOptions),
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
struct FileOptions {
    #[serde(default)]
    compression: Compression,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Compression {
    None,
    Gzip,
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
struct S3Options {
    discover_objects: (),
    #[serde(default)]
    matching: Option<String>,
    using: Vec<S3KeySource>,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
struct PostgresOptions {
    host: String,
    publication: String,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum S3KeySource {
    BucketScan(String),
    SqsNotifications(String),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::parser::parse_serde;

    #[test]
    fn test_s3_source() {
        let connector: Connector = parse_serde(
            "
            S3 DISCOVER_OBJECTS
               MATCHING 'test'
            USING
               BUCKET_SCAN 'my_bucket',
               SQS_NOTIFICATIONS 'foobar'
        ",
        );
        assert_eq!(
            connector,
            Connector::S3(S3Options {
                discover_objects: (),
                matching: Some("test".into()),
                using: vec![
                    S3KeySource::BucketScan("my_bucket".into()),
                    S3KeySource::SqsNotifications("foobar".into()),
                ],
            })
        );

        let connector: Connector = parse_serde("S3 DISCOVER_OBJECTS USING BUCKET_SCAN 'my_bucket'");
        assert_eq!(
            connector,
            Connector::S3(S3Options {
                discover_objects: (),
                matching: None,
                using: vec![S3KeySource::BucketScan("my_bucket".into())],
            })
        );
    }

    #[test]
    fn test_file_source() {
        let connector: Connector = parse_serde("FILE '/path/to/file' COMPRESSION GZIP");
        assert_eq!(
            connector,
            Connector::File(
                "/path/to/file".to_string(),
                FileOptions {
                    compression: Compression::Gzip,
                }
            )
        );

        let connector: Connector = parse_serde("FILE '/path/to/file'");
        assert_eq!(
            connector,
            Connector::File(
                "/path/to/file".to_string(),
                FileOptions {
                    compression: Compression::None,
                }
            )
        );
    }

    #[test]
    fn test_postgres_source() {
        let connector: Connector =
            parse_serde("POSTGRES HOST 'host=foobar' PUBLICATION 'my_publication'");
        assert_eq!(
            connector,
            Connector::Postgres(PostgresOptions {
                host: "host=foobar".into(),
                publication: "my_publication".into(),
            })
        );
    }
}
