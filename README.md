# terraform-provider-keboola

A [Terraform](https://www.terraform.io) Custom Provider for [Keboola Connection (KBC)](https://www.keboola.com).

[![Build Status](https://travis-ci.org/plmwong/terraform-provider-keboola.svg?branch=master)](https://travis-ci.org/plmwong/terraform-provider-keboola)

## Description

This temproray git repo is pulled from https://github.com/plmwong/terraform-provider-keboola

This is a custom terraform provider for managing common resources within the Keboola Connection (KBC) platform, such as Transformations, Orchestrations, Writers etc.

## BCIT - Keboola Current Development

Currently, the following KBC resources are being developed for configuration via `terraform`:

* `keboola_aws_redshift_writer`
* `keboola_aws_redshift_writer_table`
* `keboola_sqlserver_writer`
* `keboola_googledrive_extractor`
* `keboola_awsredshift_writer`
* `keboola_snowflake_extractor`
* `keboola_tableau_writer`
* `keboola_dropbox_writer`
* `keboola_s3bucket_writer`

These components can be created in the Keboola Connection platform and have credentials put in place but do not have table creation yet.

Todo: 
1. Create a generic extractor table provider.
2. Complete all exsiting providers.
3. Other common components will be developed in second term (Starting end of April)

## Supported Resources - Created by PayByPhone

Currently, the following KBC resources are supported (or partially supported) for configuration via `terraform`:

* `keboola_access_token`
* `keboola_csvimport_extractor`
* `keboola_gooddata_user_management`
* `keboola_gooddata_writer`
* `keboola_orchestration`
* `keboola_orchestration_tasks`
* `keboola_postgresql_writer`
* `keboola_postgresql_writer_tables`
* `keboola_snowflake_writer`
* `keboola_snowflake_writer_tables`
* `keboola_storage_bucket`
* `keboola_storage_table`
* `keboola_transformation_bucket`
* `keboola_transformation`

## Requirement

* [hashicorp/terraform](https://github.com/hashicorp/terraform)


## Singular vs. Plural

Some resources (e.g. `keboola_gooddata_writer_table`, `keboola_storage_bucket`) are configured as singular resources, while others (e.g. `keboola_snowflake_writer_tables`, `keboola_orchestration_tasks`) are plural. This is a design decision in order for
the terraform resources to closely match how they are managed through the Keboola API(s).

Resources that are plural are managed through the API in a single call that will create/update/delete all of them at once.
Resources that are singular can be created/updated/deleted independently of one another in separate API calls, and so are modelled as such.

## Usage

### Provider Configuration

The provider only requires a single configuration setting `api_key`. Make sure that the access token you use has the required permissions
for the resources that you wish to manage.

#### `keboola`

```
provider "keboola" {
  api_key     =   "${var.storage_api_key}"
}
```

### Resource Configuration

For documentation on each supported resource, refer to the [wiki](https://github.com/plmwong/terraform-provider-keboola/wiki).

## Contributing

Bug reports, suggestions, code additions/changes etc. are very welcome! When making code changes, please branch off of `master` and then
raise a pull request so it can be reviewed and merged.

## License
`terraform-provider-keboola` is provided *"as-is"* under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).



