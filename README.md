# minion

Minion is a light-weight workflow manager designed for CLI usage and maximum
customisation.

A Minion job is a YAML file that uses special tags to define a function that
can be run with parameters specified on the command line. The function defines
a pipeline that can map, for example, issues in GitHub to cards in Trello.

## Installation

```
pip install git+https://github.com/mkjpryor-stfc/minion-workflows.git
```

It is recommended to install Minion into its own Python virtual environment.
Python 3 is required (tested on Python 3.6 only).

## Usage

Once installed, the `minion` command will be available. Please use `minion --help`
to explore the available commands and options.

Of particular interest is `minion config-sources`, which will print the directories
in which Minion will search for job definitions in its current configuration.

`minion list` will list the available jobs, and `minion run` is used to run a
specific job. Job parameters can be provided using parameter files in YAML
format, or by specifying a YAML-formatted string on the command line.

In order to keep your workflows up-to-date, you can use a Cron job to execute
`minion run` commands.

A useful pattern for configuration is the following:

  * Store job definitions either in one of the directories listed by `minion config-sources`
    or in a directory specified using the `MINION_JOB_DIRS` environment variable, which
    could be set in your `.bash_profile` (or similar).
  * Define provider credentials in a single YAML file that is used by all jobs, e.g.:
    ```
    github:
      api_token: <GitHub Personal Access Token>

    trello:
      api_key: <Trello API key>
      api_token: <Trello API token>
    ```
    This file can be specified using the `MINION_PARAMS_FILES` environment variable,
    which could also be set in your `.bash_profile` (or similar).
  * Specify job-specific parameters on the command line, e.g.:
    ```
    minion run \
      -p "{trello: {board_name: 'Trello board name', list_name: 'Trello list name'}}" \
      assigned_issues_to_trello
    ```

## Defining jobs

Jobs are defined using a YAML-based DSL. For examples, see the `examples`
directory.

There are four custom tags used by Minion:

| Tag | Purpose |
| --- | --- |
| `!!minion/provider:<provider class>` | Indicates that the tagged mapping should be used as `kwargs` to create an instance of the specified class. |
| `!!minion/get_provider` | Indicates that the specified provider should be substituted. |
| `!!minion/function:<function>` | Indicates that the specified Minion function should be configured with the tagged mapping as `kwargs`. |
| `!!minion/parameter` | Indicates that the value of the specified parameter should be substituted. |
