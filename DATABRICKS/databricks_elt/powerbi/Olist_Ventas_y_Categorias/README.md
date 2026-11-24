# Informe Olist: pulso comercial

Proyecto PBIP local conectado en modo Import a las tablas Gold de Databricks. Las credenciales OAuth se guardan en Power BI Desktop y no se incluyen en Git.

## Páginas

- **Pulso comercial:** cinco indicadores, importe mensual, pedidos por estado y guía de estados, importe y flete.
- **Categorías y clientes:** Top 10 por importe y ticket promedio, ticket por las 27 UF y promedios simples por grupo. Los pedidos aparecen en tooltips.

El importe suma precio y flete registrados por línea, no el total pagado por pedido. El ticket promedio divide ese importe entre pedidos distintos. El promedio mensual incluye meses sin ventas como cero mediante un calendario local continuo. Las etiquetas se preparan en el modelo del informe; Gold y el ETL no cambian.

Abre Olist_Ventas_y_Categorias.pbip en Power BI Desktop, inicia sesión con OAuth y actualiza el modelo para incorporar las etiquetas y el calendario. No se publica en Power BI Service.

Las tarjetas incluyen iconos SVG lineales originales, guardados localmente en el proyecto.
