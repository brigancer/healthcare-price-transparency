with source_files as (
    select
        *,
        regexp_extract(_metadata.file_path, '/([0-9]{6})\\.csv$', 1) as ccn
    from read_files(
        '/Volumes/workspace/hpt_dev/bronze_mrf_raw',
        format => 'csv',
        header => 'true',
        skipRows => 2,
        pathGlobFilter => '*.csv',
        inferSchema => 'true'
    )
    where regexp_extract(_metadata.file_path, '/([0-9]{6})\\.csv$', 1) 
            not in ('440218', '440082', '440133', '440053')
),

shoppable_only as (
    select
    sf.*
    from source_files sf
    inner join {{ ref('shoppable_services') }} ss
    on ss.code = sf.`code|1`
    or ss.code = sf.`code|2`
    or ss.code = sf.`code|3`
    or ss.code = sf.`code|4`
)

-- temporary select to clear linter error message 
select * from shoppable_only limit 5