# -*- coding: utf-8 -*-
"""Genera preview_tts.html para validar el boton de voz en el navegador.
Uso: python preview_tts.py  ->  abre preview_tts.html en Chrome/Edge/Safari.
preview_tts.html es un artefacto generado; NO commitearlo."""
from daily_briefing import build_standalone_html_report

INNER_DEMO = """
<p class="turn"><b class="turn-b">En contexto —</b> El foco de ayer en el rebote de Micron se revirtio hoy: la escasez de chips de memoria empujo a Apple a subir precios y desato un sell-off tecnologico que arrastra los futuros del Nasdaq.</p>

<div class="brief"><div class="brief-lbl">El dia en tres lineas</div>
<ol class="brief-ol">
<li class="brief-li"><b>El RAMageddon castiga a la tech global:</b> la escasez de chips de memoria hoy es el riesgo, con Apple subiendo precios y los semiconductores liderando las caidas.</li>
<li class="brief-li"><b>La Fed de Warsh frena al mercado:</b> el Treasury a 10 anos baja a 4,37% en busca de refugio y el dolar se consolida.</li>
<li class="brief-li"><b>Chile entre la reforma y el desempleo:</b> el Senado aprobo por un voto la reforma tributaria que vuelve a Hacienda para ajustes.</li>
</ol></div>

<div class="watch"><div class="watch-lbl">Que mirar hoy</div>
<ul class="watch-ul">
<li class="watch-li">Datos de sentimiento del consumidor en EE.UU. (Universidad de Michigan, revision final de junio).</li>
<li class="watch-li">Estrecho de Ormuz: la ONU pauso su plan de evacuacion de buques, con impacto en petroleo.</li>
</ul></div>

<div class="part">La historia del dia</div>
<div class="sec"><p class="lead-item"><b>La escasez de chips se convirtio en el arma de doble filo del mercado.</b> Apple anuncio alzas de precios en Mac e iPad por el desabastecimiento de RAM y sus acciones cayeron, arrastrando al sector tecnologico global. Los fabricantes surcoreanos se desplomaron hasta 6% en Seul. <a class="src" href="#">Bloomberg</a></p></div>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;margin:18px 0 4px 0;font-family:sans-serif;">
<tr><th style="text-align:left;padding:6px 0;color:#80868b;">ACTIVO</th><th style="text-align:right;padding:6px 0;color:#80868b;">PRECIO</th></tr>
<tr><td style="padding:6px 0;font-weight:600;">S&P 500</td><td style="text-align:right;padding:6px 0;">7.414,2</td></tr>
<tr><td style="padding:6px 0;font-weight:600;">USD/CLP</td><td style="text-align:right;padding:6px 0;">923,7</td></tr>
</table>

<div class="part">Internacional</div>
<div class="sec"><div class="eyebrow">Fed, inflacion y renta fija</div><p class="item"><b>Kashkari abre la puerta a un alza de tasas y los economistas lo contradicen.</b> El presidente de la Fed de Minneapolis senalo que espera que la Fed suba las tasas este ano, mientras una encuesta de Reuters proyecta tasas sin cambios. <a class="src" href="#">Reuters</a></p></div>

<div class="part">Tambien, en breve</div>
<ul class="bin">
<li class="bin-bullet"><b>Bitcoin toca minimos de 21 meses</b> cerca de US$59.500, acumulando -19,1% en el mes.</li>
<li class="bin-bullet"><b>Chile se autodeclaro Pais Libre de Influenza Aviar</b> en aves de corral, abriendo mercados de exportacion.</li>
</ul>
"""

html = build_standalone_html_report(INNER_DEMO, "Viernes 26 de junio de 2026", "Morning Brief")
with open("preview_tts.html", "w", encoding="utf-8") as f:
    f.write(html)
print("OK -> preview_tts.html (abrelo en Chrome/Edge/Safari y prueba 'Leer informe')")
