"""Construye un modelo estrella incremental desde CDF de Silver."""

from __future__ import annotations

from collections.abc import Mapping
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, functions as F

from utils.cdf import (
    TableVersion,
    get_watermark,
    latest_change_images,
    latest_table_version,
    read_cdf_range,
    save_watermark,
)
from utils.logging_config import get_logger
from utils.merge import merge_idempotently

LOGGER = get_logger(__name__)


def _read_inputs(
    spark: SparkSession,
    common_config: Mapping[str, str],
    gold_config: Mapping[str, object],
    watermark_table: str,
) -> tuple[
    dict[str, str],
    dict[str, TableVersion],
    dict[str, TableVersion | None],
    dict[str, DataFrame | None],
    bool,
]:
    catalog = common_config["catalog"]
    source_schema = str(gold_config["source_schema"]).format(**common_config)
    source_tables = gold_config["source_tables"]
    source_names = {
        key: f"{catalog}.{source_schema}.{table_config['table']}"
        for key, table_config in source_tables.items()
    }
    latest_versions = {
        key: latest_table_version(spark, name)
        for key, name in source_names.items()
    }
    watermarks = {
        key: get_watermark(spark, watermark_table, name)
        for key, name in source_names.items()
    }
    bootstrap = any(version is None for version in watermarks.values())
    changes: dict[str, DataFrame | None] = {}

    for key, name in source_names.items():
        latest = latest_versions[key].version
        watermark = watermarks[key]
        if bootstrap:
            LOGGER.info("%s: Carga inicial desde la versión %s", name, latest)
            changes[key] = (
                spark.read.option("versionAsOf", latest)
                .table(name)
            )
        elif watermark is not None and latest > watermark.version:
            start = watermark.version + 1
            LOGGER.info("%s: Leyendo CDF de la versión %s a la %s", name, start, latest)
            changed = latest_change_images(
                read_cdf_range(spark, name, start, latest),
                list(source_tables[key]["primary_keys"]),
            )
            changes[key] = None if changed.isEmpty() else changed
            if changes[key] is None:
                LOGGER.info("%s: CDF sin cambios elegibles; se actualiza el watermark", name)
        else:
            LOGGER.info("%s: Sin cambios desde la versión %s", name, latest)
            changes[key] = None

    return source_names, latest_versions, watermarks, changes, bootstrap


def _union_keys(frames: list[DataFrame], column: str) -> DataFrame:
    if not frames:
        raise ValueError(f"Se requiere al menos un origen para {column}")
    return reduce(
        lambda left, right: left.unionByName(right),
        [frame.select(column) for frame in frames],
    ).dropDuplicates([column])


def _product_ids(
    current_products: DataFrame,
    changes: Mapping[str, DataFrame | None],
    bootstrap: bool,
) -> DataFrame | None:
    if bootstrap:
        return current_products.select("product_id").dropDuplicates(["product_id"])

    frames = []
    if changes["products"] is not None:
        frames.append(changes["products"])
    if changes["category_translation"] is not None:
        changed_categories = changes["category_translation"].select(
            "product_category_name"
        )
        frames.append(
            current_products.join(changed_categories, "product_category_name")
        )
    if not frames:
        return None
    return _union_keys(frames, "product_id")


def _build_product_dimension(
    current_products: DataFrame,
    current_translation: DataFrame,
    product_ids: DataFrame,
) -> DataFrame:
    return (
        current_products.join(product_ids, "product_id")
        .join(current_translation, "product_category_name", "left")
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
    )


def _affected_orders(
    current_orders: DataFrame,
    current_items: DataFrame,
    current_products: DataFrame,
    changes: Mapping[str, DataFrame | None],
    product_ids: DataFrame | None,
    bootstrap: bool,
) -> DataFrame | None:
    if bootstrap:
        return current_orders.select("order_id").dropDuplicates(["order_id"])

    frames = []
    for key in ("orders", "order_items"):
        if changes[key] is not None:
            frames.append(changes[key].select("order_id"))

    if changes["customers"] is not None:
        customer_ids = changes["customers"].select("customer_id")
        frames.append(
            current_orders.join(customer_ids, "customer_id").select("order_id")
        )

    if product_ids is not None:
        frames.append(current_items.join(product_ids, "product_id").select("order_id"))

    if changes["sellers"] is not None:
        seller_ids = changes["sellers"].select("seller_id")
        frames.append(current_items.join(seller_ids, "seller_id").select("order_id"))

    if changes["category_translation"] is not None and product_ids is None:
        categories = changes["category_translation"].select("product_category_name")
        changed_products = current_products.join(
            categories, "product_category_name"
        ).select("product_id")
        frames.append(
            current_items.join(changed_products, "product_id").select("order_id")
        )

    if not frames:
        return None
    return _union_keys(frames, "order_id")


def _build_date_dimension(order_dates: DataFrame) -> DataFrame:
    dates = order_dates.select(
        F.to_date("order_purchase_timestamp").alias("full_date")
    ).filter(F.col("full_date").isNotNull()).dropDuplicates(["full_date"])
    return dates.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.dayofmonth("full_date").alias("day"),
        F.dayofweek("full_date").alias("day_of_week"),
    )


def _build_order_item_fact(
    current_orders: DataFrame,
    current_items: DataFrame,
    affected_order_ids: DataFrame,
) -> DataFrame:
    item_price = F.col("price")
    freight_value = F.col("freight_value")
    return (
        current_items.join(current_orders, "order_id")
        .join(affected_order_ids, "order_id")
        .select(
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "seller_id",
            F.date_format(
                F.to_date("order_purchase_timestamp"), "yyyyMMdd"
            ).cast("int").alias("order_date_key"),
            "order_status",
            item_price.cast("decimal(12, 2)").alias("item_price"),
            freight_value.cast("decimal(12, 2)").alias("freight_value"),
            (
                F.coalesce(item_price, F.lit(0))
                + F.coalesce(freight_value, F.lit(0))
            ).cast("decimal(13, 2)").alias("total_item_amount"),
            F.lit(1).cast("int").alias("item_quantity"),
        )
    )


def transform_all(
    spark: SparkSession,
    common_config: Mapping[str, str],
    gold_config: Mapping[str, object],
) -> bool:
    """Actualiza el modelo estrella y sus watermarks desde CDF de Silver."""
    catalog = common_config["catalog"]
    schema = str(gold_config["target_schema"]).format(**common_config)
    watermark_table = (
        f"{catalog}.{common_config['control_schema']}."
        f"{common_config['gold_cdf_watermarks_table']}"
    )
    source_names, latest_versions, watermarks, changes, bootstrap = _read_inputs(
        spark, common_config, gold_config, watermark_table
    )

    def advance_watermarks() -> None:
        for key, source_name in source_names.items():
            watermark = watermarks[key]
            if bootstrap or (watermark is not None and latest_versions[key].version > watermark.version):
                target_table = gold_config["source_tables"][key]["gold_target"]
                save_watermark(
                    spark, watermark_table, source_name,
                    f"{catalog}.{schema}.{target_table}", latest_versions[key],
                )

    if not bootstrap and not any(value is not None for value in changes.values()):
        advance_watermarks()
        return False

    if bootstrap:
        LOGGER.info("Cargando el modelo estrella desde el estado actual de Silver")
        current = changes
    else:
        current = {
            key: spark.table(source_name)
            for key, source_name in source_names.items()
        }

    current_orders = current["orders"]
    current_items = current["order_items"]
    current_products = current["products"]
    current_translation = current["category_translation"]
    target_names = {
        "customer": f"{catalog}.{schema}.dim_customer",
        "product": f"{catalog}.{schema}.dim_product",
        "seller": f"{catalog}.{schema}.dim_seller",
        "date": f"{catalog}.{schema}.dim_date",
        "fact": f"{catalog}.{schema}.fct_order_items",
    }

    if bootstrap or changes["customers"] is not None:
        customers = changes["customers"].select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        )
        merge_idempotently(
            spark, customers, target_names["customer"], ["customer_id"]
        )

    if bootstrap or changes["sellers"] is not None:
        sellers = changes["sellers"].select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        )
        merge_idempotently(spark, sellers, target_names["seller"], ["seller_id"])

    product_ids = _product_ids(current_products, changes, bootstrap)
    if product_ids is not None:
        products = _build_product_dimension(
            current_products, current_translation, product_ids
        )
        merge_idempotently(spark, products, target_names["product"], ["product_id"])

    affected_orders = _affected_orders(
        current_orders,
        current_items,
        current_products,
        changes,
        product_ids,
        bootstrap,
    )
    if affected_orders is not None:
        order_dates = current_orders.join(affected_orders, "order_id")
        dates = _build_date_dimension(order_dates)
        if not dates.isEmpty():
            merge_idempotently(spark, dates, target_names["date"], ["date_key"])

        facts = _build_order_item_fact(current_orders, current_items, affected_orders)
        if not facts.isEmpty():
            merge_idempotently(
                spark, facts, target_names["fact"], ["order_id", "order_item_id"]
            )

    advance_watermarks()
    LOGGER.info("Terminó la actualización incremental del modelo estrella Gold")
    return True
