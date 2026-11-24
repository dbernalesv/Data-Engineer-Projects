# Sustentación Olist

Fuente LaTeX de la presentación Beamer del proyecto ELT de Olist. Está pensada para una exposición de 15 minutos, con la ejecución de enero de 2019 y la actualización manual del modelo Import de Power BI como demostración.

## Compilar

Usa MiKTeX o TeX Live con Beamer, TikZ y PGFPlots. Desde esta carpeta ejecuta dos veces:

```powershell
pdflatex -interaction=nonstopmode -halt-on-error main.tex
pdflatex -interaction=nonstopmode -halt-on-error main.tex
```

El PDF resultante será `main.pdf`. El código fuente no depende de archivos fuera de esta carpeta.

## Archivos

- `main.tex`: diapositivas, diagramas y gráficos vectoriales.
- `assets/`: capturas de las dos páginas del informe Power BI, con filtro diciembre de 2018.

La extensión de noviembre y diciembre de 2018 se identifica como sintética. El lote de enero de 2019 se agregará a `inbox/` solo después de generarlo y validar sus valores esperados; sus cifras no se anticipan en estas diapositivas.
