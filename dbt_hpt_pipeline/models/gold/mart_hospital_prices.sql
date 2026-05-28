with mrf_prices as (
    select
        ccn,
        description,
        code_1,
        code_1_type,
        code_2,
        code_2_type,
        code_3,
        code_3_type,
        code_4,
        code_4_type,
        billing_class,
        setting,
        modifiers,
        drug_unit_of_measurement,
        drug_type_of_measurement,
        additional_generic_notes,
        payer_name,
        plan_name,
        negotiated_algorithm,
        methodology,
        additional_payer_notes,
        negotiated_dollar,
        negotiated_percentage,
        standard_charge_gross,
        standard_charge_discounted_cash,
        standard_charge_min,
        standard_charge_max,
        median_amount,
        10th_percentile,
        90th_percentile,
        count
    from {{ ref('stg_mrf_prices') }}
),

hospitals as (
    select
        ccn,
        hospital_name,
        system,
        city,
        county
    from {{ ref('nashville_msa_hospitals') }}
),

shoppable_services as (
    select
        code,
        code_type,
        category,
        description as service_description
    from {{ ref('shoppable_services') }}
)

select
    -- Hospital identity
    mrf_prices.ccn,
    hospitals.hospital_name,
    hospitals.system,
    hospitals.city,
    hospitals.county,

    -- Service identity
    mrf_prices.code_1,
    mrf_prices.code_1_type,
    shoppable_services.category,
    shoppable_services.service_description,
    mrf_prices.description                          as hospital_service_description,
    mrf_prices.billing_class,
    mrf_prices.setting,
    mrf_prices.modifiers,
    mrf_prices.drug_unit_of_measurement,
    mrf_prices.drug_type_of_measurement,
    mrf_prices.additional_generic_notes,

    -- Payer
    mrf_prices.payer_name,
    mrf_prices.plan_name,
    mrf_prices.methodology,
    mrf_prices.additional_payer_notes,
    mrf_prices.negotiated_algorithm,

    -- Negotiated rates
    mrf_prices.negotiated_dollar,
    mrf_prices.negotiated_percentage,

    -- Standard charges
    mrf_prices.standard_charge_gross,
    mrf_prices.standard_charge_discounted_cash,
    mrf_prices.standard_charge_min,
    mrf_prices.standard_charge_max,

    -- Statistical rates
    mrf_prices.median_amount,
    mrf_prices.10th_percentile,
    mrf_prices.90th_percentile,
    mrf_prices.count

from mrf_prices
inner join hospitals
    on mrf_prices.ccn = hospitals.ccn
inner join shoppable_services
    on cast(mrf_prices.code_1 as string) = cast(shoppable_services.code as string)
    and (
        mrf_prices.code_1_type = shoppable_services.code_type
        or (mrf_prices.code_1_type like '%DRG%' and shoppable_services.code_type = 'DRG')
    )