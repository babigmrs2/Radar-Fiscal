"""
coletor.py — Coletor automatizado do Radar Fiscal Diário
Coleta notícias recentes de feeds RSS públicos e portais oficiais
relacionados à Reforma Tributária, agronegócio e fertilizantes — com
cobertura estadual nos 10 estados prioritários do agronegócio — filtra
por palavras-chave, classifica cada notícia (esfera / uf / setor / impacto)
e mantém um histórico rotativo de 7 dias em noticias.json, no formato
consumido pelo index.html.
Instalação das dependências:
   pip install requests feedparser beautifulsoup4
Execução manual:
   python coletor.py
Agendamento:
   - cron (Linux/Mac), rodando todo dia às 7h:
       0 7 * * * /usr/bin/python3 /caminho/completo/coletor.py >> /caminho/completo/coletor.log 2>&1
   - Windows Task Scheduler:
       Ação: python.exe  Argumentos: C:\\caminho\\coletor.py  Iniciar em: C:\\caminho
   - GitHub Actions (.github/workflows/radar.yml):
       on:
         schedule:
           - cron: '0 10 * * *'   # 07:00 horário de Brasília
       jobs:
         coletar:
           runs-on: ubuntu-latest
           steps:
             - uses: actions/checkout@v4
             - uses: actions/setup-python@v5
               with: {python-version: '3.11'}
             - run: pip install requests feedparser beautifulsoup4
             - run: python coletor.py
             - run: |
                 git config user.name "radar-bot"
                 git config user.email "radar-bot@users.noreply.github.com"
                 git add noticias.json
                 git commit -m "Atualiza noticias.json" || echo "Sem mudanças"
                 git push
"""
import json
import logging
import re
from datetime import datetime, timedelta, timezone, date
from typing import Optional
import feedparser
import requests
from bs4 import BeautifulSoup
logging.basicConfig(
   level=logging.INFO,
   format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("coletor")
TIMEOUT_SEGUNDOS = 12
JANELA_COLETA_HORAS = 48       # notícias mais antigas que isso não entram na coleta desta execução
RETENCAO_DIAS = 7              # histórico mantido no noticias.json
LIMITE_ITENS = 35              # itens mais recentes mantidos após a mesclagem
SAIDA_JSON = "noticias.json"
# ---------------------------------------------------------------------------
# Fontes federais / da Reforma Tributária
# ---------------------------------------------------------------------------
FONTES_FEDERAIS = [
   {"nome": "Agência Senado", "url": "https://www12.senado.leg.br/noticias/rss/ultimas-noticias", "esfera_padrao": "reforma", "uf": None},
   {"nome": "Câmara dos Deputados", "url": "https://www.camara.leg.br/noticias/rss.xml", "esfera_padrao": "reforma", "uf": None},
   {"nome": "Agência Brasil - Economia", "url": "https://agenciabrasil.ebc.com.br/rss/economia/feed.xml", "esfera_padrao": "federal", "uf": None},
   {"nome": "Portal Contábeis", "url": "https://www.contabeis.com.br/rss/noticias/", "esfera_padrao": "federal", "uf": None},
]
# ---------------------------------------------------------------------------
# Fontes estaduais — 10 estados prioritários do agronegócio.
# Ajuste/adicione URLs conforme a disponibilidade real de cada fonte: os
# endereços abaixo seguem o padrão esperado de RSS de Sefaz/Diário Oficial,
# mas cada estado publica em domínios e caminhos próprios.
# ---------------------------------------------------------------------------
FONTES_ESTADUAIS = [
   {"nome": "Sefaz-RS", "url": "https://www.sefaz.rs.gov.br/rss/noticias", "esfera_padrao": "estadual", "uf": "RS"},
   {"nome": "SEF/SC", "url": "https://www.sef.sc.gov.br/rss/noticias", "esfera_padrao": "estadual", "uf": "SC"},
   {"nome": "SEFA-PR / Diário Oficial PR", "url": "https://www.aen.pr.gov.br/rss", "esfera_padrao": "estadual", "uf": "PR"},
   {"nome": "Sefaz-SP / Diário Oficial SP", "url": "https://www.doe.sp.gov.br/rss", "esfera_padrao": "estadual", "uf": "SP"},
   {"nome": "Sefaz-MG / Jornal Minas Gerais", "url": "https://www.jornalminasgerais.mg.gov.br/rss", "esfera_padrao": "estadual", "uf": "MG"},
   {"nome": "Economia-GO (Sefaz-GO)", "url": "https://www.economia.go.gov.br/rss/noticias", "esfera_padrao": "estadual", "uf": "GO"},
   {"nome": "Sefaz-MS", "url": "https://www.sefaz.ms.gov.br/rss/noticias", "esfera_padrao": "estadual", "uf": "MS"},
   {"nome": "Sefaz-MT", "url": "https://www.sefaz.mt.gov.br/rss/noticias", "esfera_padrao": "estadual", "uf": "MT"},
   {"nome": "Sefaz-MA", "url": "https://www.sefaz.ma.gov.br/rss/noticias", "esfera_padrao": "estadual", "uf": "MA"},
   {"nome": "Sefaz-TO", "url": "https://www.sefaz.to.gov.br/rss/noticias", "esfera_padrao": "estadual", "uf": "TO"},
]
FONTES = FONTES_FEDERAIS + FONTES_ESTADUAIS
# ---------------------------------------------------------------------------
# Mapa de UFs: código, nome por extenso, gentílico(s) e órgãos locais.
# Usado para identificar o estado mencionado no título/resumo da notícia,
# mesmo quando a sigla da UF não aparece explicitamente.
# ---------------------------------------------------------------------------
UF_MAPA = {
   "RS": {"nome": "Rio Grande do Sul", "gentilicos": ["gaúcho", "gaúcha"], "orgaos": ["sefaz-rs", "sefaz rs", "receita estadual do rs"]},
   "SC": {"nome": "Santa Catarina", "gentilicos": ["catarinense"], "orgaos": ["sef/sc", "sef sc", "fazenda catarinense"]},
   "PR": {"nome": "Paraná", "gentilicos": ["paranaense"], "orgaos": ["sefa-pr", "sefa pr", "receita estadual do paraná"]},
   "SP": {"nome": "São Paulo", "gentilicos": ["paulista"], "orgaos": ["sefaz-sp", "sefaz sp", "fazenda paulista"]},
   "MG": {"nome": "Minas Gerais", "gentilicos": ["mineiro", "mineira"], "orgaos": ["sefaz-mg", "sefaz mg", "fazenda mineira"]},
   "GO": {"nome": "Goiás", "gentilicos": ["goiano", "goiana"], "orgaos": ["sefaz-go", "sefaz go", "economia-go", "secretaria da economia de goiás"]},
   "MS": {"nome": "Mato Grosso do Sul", "gentilicos": ["sul-mato-grossense"], "orgaos": ["sefaz-ms", "sefaz ms"]},
   "MT": {"nome": "Mato Grosso", "gentilicos": ["mato-grossense"], "orgaos": ["sefaz-mt", "sefaz mt"]},
   "MA": {"nome": "Maranhão", "gentilicos": ["maranhense"], "orgaos": ["sefaz-ma", "sefaz ma"]},
   "TO": {"nome": "Tocantins", "gentilicos": ["tocantinense"], "orgaos": ["sefaz-to", "sefaz to"]},
}
# Palavras/termos de busca. Termos curtos (siglas, sem espaço) são validados
# com \b...\b para não colidir como substring de outra palavra; termos com
# espaço (frases) seguem checagem por substring simples, que já é segura.
PALAVRAS_CHAVE = [
   "fertilizantes", "adubos", "reforma tributária", "ibs", "cbs",
   "imposto seletivo", "crédito presumido", "icms convênio",
   "insumos agrícolas", "funrural",
]
GATILHOS_IMPACTO_ALTO = [
   "alíquota", "aumento de imposto", "revogação", "prazo final",
   "vigência imediata", "perda de crédito", "obrigatoriedade",
]
GATILHOS_IMPACTO_MEDIO = [
   "consulta pública", "consulta", "jurisprudência", "julgamento",
   "convênio", "prorrogação",
]
# Tudo que não bater nos gatilhos acima cai em "baixo" (conceitual / layout de sistema)
PALAVRAS_FERTILIZANTES = ["fertilizante", "adubo", "npk", "ureia", "potássio", "fosfato", "amônia", "enxofre"]
PALAVRAS_MUNICIPAL = ["prefeitura", "município", "iss ", "issqn", "nfs-e"]
PALAVRAS_ESTADUAL = ["icms", "sefaz", "convênio", "secretaria da fazenda"]
PALAVRAS_REFORMA = ["reforma tributária", "imposto seletivo", "comitê gestor"]
# ibs/cbs tratados à parte via regex com \b, pois são siglas curtas

def termo_bate_com_limite_de_palavra(termo: str, texto_lower: str) -> bool:
   """Valida um termo curto (sigla) com limites de palavra, evitando falso
   positivo como substring de outra palavra (ex.: 'GO' dentro de 'GOL')."""
   return re.search(rf"\b{re.escape(termo)}\b", texto_lower, re.IGNORECASE) is not None

def texto_contem_palavra_chave(texto: str) -> bool:
   texto_lower = texto.lower()
   for palavra in PALAVRAS_CHAVE:
       if " " in palavra or len(palavra) > 4:
           if palavra in texto_lower:
               return True
       else:
           if termo_bate_com_limite_de_palavra(palavra, texto_lower):
               return True
   return False

def limpar_html(texto: str) -> str:
   """Remove tags HTML de resumos de RSS, retornando texto puro e enxuto."""
   if not texto:
       return ""
   texto_puro = BeautifulSoup(texto, "html.parser").get_text(separator=" ")
   texto_puro = re.sub(r"\s+", " ", texto_puro).strip()
   return texto_puro[:280]

def detectar_uf_por_texto(texto: str) -> Optional[str]:
   """Procura menção a um dos 10 estados prioritários no texto: sigla (com
   limite de palavra), nome por extenso, gentílico ou órgão/Sefaz local."""
   texto_lower = texto.lower()
   for sigla, dados in UF_MAPA.items():
       if termo_bate_com_limite_de_palavra(sigla, texto_lower):
           return sigla
       if dados["nome"].lower() in texto_lower:
           return sigla
       if any(g in texto_lower for g in dados["gentilicos"]):
           return sigla
       if any(o in texto_lower for o in dados["orgaos"]):
           return sigla
   return None

def classificar_uf(texto: str, uf_da_fonte: Optional[str]) -> Optional[str]:
   """Se a fonte já é a Sefaz/canal oficial de um estado, usa essa UF
   diretamente; caso contrário, tenta identificar pelo conteúdo do texto."""
   if uf_da_fonte:
       return uf_da_fonte
   return detectar_uf_por_texto(texto)

def classificar_esfera(texto: str, esfera_padrao: str, uf: Optional[str]) -> str:
   texto_lower = texto.lower()
   if any(p in texto_lower for p in PALAVRAS_REFORMA):
       return "reforma"
   if termo_bate_com_limite_de_palavra("ibs", texto_lower) or termo_bate_com_limite_de_palavra("cbs", texto_lower):
       return "reforma"
   if any(p in texto_lower for p in PALAVRAS_MUNICIPAL):
       return "municipal"
   if uf is not None or any(p in texto_lower for p in PALAVRAS_ESTADUAL):
       return "estadual"
   return esfera_padrao if esfera_padrao in ("federal", "estadual", "municipal", "reforma") else "federal"

def classificar_setor(texto: str) -> str:
   texto_lower = texto.lower()
   if any(p in texto_lower for p in PALAVRAS_FERTILIZANTES):
       return "fertilizantes"
   return "agronegocio"

def classificar_impacto(texto: str) -> str:
   texto_lower = texto.lower()
   if any(p in texto_lower for p in GATILHOS_IMPACTO_ALTO):
       return "alto"
   if any(p in texto_lower for p in GATILHOS_IMPACTO_MEDIO):
       return "medio"
   return "baixo"

def buscar_feed(url: str) -> Optional[feedparser.FeedParserDict]:
   """Baixa e faz parse de um feed RSS, com tratamento de timeout/erro de rede."""
   try:
       resposta = requests.get(url, timeout=TIMEOUT_SEGUNDOS, headers={"User-Agent": "RadarFiscalBot/1.0"})
       resposta.raise_for_status()
       return feedparser.parse(resposta.content)
   except requests.exceptions.Timeout:
       log.warning("Timeout ao acessar %s", url)
   except requests.exceptions.RequestException as erro:
       log.warning("Falha ao acessar %s: %s", url, erro)
   return None

def data_publicacao(entrada) -> Optional[datetime]:
   """Extrai a data de publicação de uma entrada de feed, se disponível."""
   for campo in ("published_parsed", "updated_parsed"):
       valor = getattr(entrada, campo, None)
       if valor:
           return datetime(*valor[:6], tzinfo=timezone.utc)
   return None

def dentro_da_janela_de_coleta(dt_publicacao: Optional[datetime], horas: int = JANELA_COLETA_HORAS) -> bool:
   """Se a data não estiver disponível, mantemos a notícia (melhor incluir do que perder)."""
   if dt_publicacao is None:
       return True
   limite = datetime.now(timezone.utc) - timedelta(hours=horas)
   return dt_publicacao >= limite

def coletar() -> list:
   """Percorre todas as fontes (federais + 10 estados) e retorna a lista de
   notícias novas já filtradas por palavra-chave e classificadas."""
   coletadas = []
   proximo_id = 1
   for fonte in FONTES:
log.info("Buscando feed: %s", fonte["nome"])
       feed = buscar_feed(fonte["url"])
       if feed is None or not getattr(feed, "entries", None):
           log.warning("Sem entradas para %s — pulando fonte.", fonte["nome"])
           continue
       for entrada in feed.entries:
           titulo = getattr(entrada, "title", "").strip()
           link = getattr(entrada, "link", "").strip()
           resumo_bruto = getattr(entrada, "summary", "") or getattr(entrada, "description", "")
           resumo = limpar_html(resumo_bruto)
           if not titulo or not link:
               continue
           texto_completo = f"{titulo} {resumo}"
           if not texto_contem_palavra_chave(texto_completo):
               continue
           dt_pub = data_publicacao(entrada)
           if not dentro_da_janela_de_coleta(dt_pub):
               continue
           uf = classificar_uf(texto_completo, fonte["uf"])
           item = {
               "id": proximo_id,
               "titulo": titulo,
               "resumo": resumo or "Resumo não disponível na fonte original.",
               "setor": classificar_setor(texto_completo),
               "esfera": classificar_esfera(texto_completo, fonte["esfera_padrao"], uf),
               "uf": uf,
               "impacto": classificar_impacto(texto_completo),
               "fonte": fonte["nome"],
               "link": link,
               "data": (dt_pub or datetime.now(timezone.utc)).strftime("%Y-%m-%d"),
           }
           coletadas.append(item)
           proximo_id += 1
   return coletadas

def remover_duplicatas(itens: list) -> list:
   """Remove notícias com o mesmo link, mantendo a primeira ocorrência da
   lista (o chamador decide a ordem de prioridade antes de passar aqui)."""
   vistos = set()
   unicos = []
   for item in itens:
       if item["link"] in vistos:
           continue
       vistos.add(item["link"])
       unicos.append(item)
   return unicos

def carregar_existentes(caminho: str = SAIDA_JSON) -> list:
   """Carrega o noticias.json já salvo em execuções anteriores, se existir."""
   try:
       with open(caminho, "r", encoding="utf-8") as arquivo:
           dados = json.load(arquivo)
           return dados if isinstance(dados, list) else []
   except FileNotFoundError:
log.info("Nenhum %s preexistente — iniciando histórico do zero.", caminho)
       return []
   except json.JSONDecodeError:
       log.warning("%s existente está corrompido — iniciando histórico do zero.", caminho)
       return []

def data_dentro_da_retencao(item: dict, dias: int = RETENCAO_DIAS) -> bool:
   """True se a data do item (YYYY-MM-DD) estiver dentro da janela de retenção."""
   try:
       data_item = datetime.strptime(item["data"], "%Y-%m-%d").date()
   except (KeyError, ValueError):
       return False
   limite = date.today() - timedelta(days=dias)
   return data_item >= limite

def mesclar_com_historico(existentes: list, novas: list, dias: int = RETENCAO_DIAS, limite_itens: int = LIMITE_ITENS) -> list:
   """Mescla notícias já salvas com as recém-coletadas, deduplica por link
   (priorizando a versão mais nova em caso de conflito), descarta itens fora
   da janela de retenção e mantém apenas os mais recentes."""
   combinadas = remover_duplicatas(novas + existentes)  # novas primeiro: vencem em caso de link repetido
   combinadas = [item for item in combinadas if data_dentro_da_retencao(item, dias)]
   combinadas.sort(key=lambda item: item.get("data", ""), reverse=True)
   combinadas = combinadas[:limite_itens]
   # renumera ids sequencialmente após a mesclagem
   for novo_id, item in enumerate(combinadas, start=1):
       item["id"] = novo_id
   return combinadas

def salvar_json(itens: list, caminho: str = SAIDA_JSON) -> None:
   with open(caminho, "w", encoding="utf-8") as arquivo:
       json.dump(itens, arquivo, ensure_ascii=False, indent=2)
log.info("Salvo %s com %d notícias (histórico de %d dias).", caminho, len(itens), RETENCAO_DIAS)

def main() -> None:
log.info("Iniciando coleta do Radar Fiscal Diário (federal + 10 estados prioritários)...")
   novas = coletar()
   novas = remover_duplicatas(novas)
log.info("Coleta desta execução: %d notícias novas relevantes.", len(novas))
   existentes = carregar_existentes()
   combinadas = mesclar_com_historico(existentes, novas)
   if not combinadas:
       log.warning("Nenhuma notícia relevante (nova ou em histórico) — noticias.json não foi sobrescrito.")
       return
   salvar_json(combinadas)
log.info("Coleta finalizada com sucesso.")

if __name__ == "__main__":
   main()