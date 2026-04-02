// ============================================================
// ADEQ TOOLBAR — Keywords & Frases Semilla
// Multi-categoría, multi-idioma, con rotación aleatoria.
// ============================================================

export const LANGUAGES = {
  en: "Inglés",
  es: "Español",
  pt: "Portugués",
  it: "Italiano",
  fr: "Francés",
  ar: "Árabe",
};

export const KEYWORDS = {
  en: [
    // News & Politics
    "breaking news","world news","politics today","government policy","election results","democracy","congress","senate","parliament","foreign policy","international news","local news","investigative journalism","opinion editorial","fact check",
    // Sports
    "football scores","soccer highlights","basketball news","baseball scores","tennis results","golf tournament","cricket match","rugby league","formula 1 race","nascar standings","nfl week","nba playoffs","mlb standings","nhl scores","premier league table","champions league draw","world cup 2026","boxing fight","mma results","esports tournament","fantasy sports tips","sports betting odds","olympic games","athletics championship",
    // Finance & Economy
    "stock market today","cryptocurrency prices","bitcoin news","ethereum update","investing tips","day trading","forex signals","personal finance","mortgage rates","insurance quotes","retirement planning","wall street news","nasdaq index","dow jones today","commodities prices","gold price","real estate market","fintech startup","venture capital","IPO news",
    // Technology
    "artificial intelligence news","machine learning trends","cybersecurity threats","startup funding","software development","hardware reviews","smartphone comparison","iphone review","android update","cloud computing","blockchain technology","metaverse news","social media trends","tiktok viral","instagram reels","youtube creators","silicon valley","big tech regulation","app development","gaming industry",
    // Entertainment
    "new movies 2025","film reviews","cinema releases","tv series review","netflix new shows","streaming services","hbo max","disney plus","music charts","concert tickets","celebrity news","awards show","oscars 2025","grammys nominees","emmys results","pop culture","viral videos","memes today","influencer news","podcast recommendations","comedy shows","drama series","documentary films",
    // Lifestyle & Health
    "healthy recipes","cooking tips","restaurant reviews","diet plans","nutrition guide","fitness workout","yoga routine","gym exercises","mental health tips","wellness habits","travel destinations","vacation ideas","hotel reviews","luxury travel","budget travel","fashion trends","beauty tips","skincare routine","makeup tutorial","parenting advice","pet care","home decor ideas","interior design","gardening tips","diy projects","electric vehicles","car reviews",
    // Science & Education
    "space exploration","nasa news","climate change","environmental news","medical research","cancer treatment","diabetes management","vaccine news","psychology tips","biology discoveries","physics breakthroughs","chemistry news","ocean conservation","wildlife protection","university rankings","online courses","e-learning platforms","science discoveries",
    // Regional
    "usa today news","uk news update","australia news","canada news","india today","south africa news","middle east news","latin america news",
    // Business & Marketing
    "digital marketing tips","seo strategies","content marketing","social media marketing","email marketing","ecommerce trends","dropshipping","amazon fba","affiliate marketing","online business","entrepreneurship","small business tips","b2b marketing","lead generation","startup ideas",
    // Gambling & Betting
    "sports betting tips","casino games","poker strategy","online gambling","horse racing odds","lottery results","fantasy football picks","esports betting",
    // Automotive
    "car news","electric car reviews","tesla news","autonomous vehicles","motorcycle news","car maintenance tips","used cars","new car models 2025",
    // Real Estate
    "real estate investing","property news","housing market","buy house tips","rent vs buy","commercial real estate","airbnb tips","mortgage calculator",
    // Food & Cooking
    "restaurant guide","food delivery","vegan recipes","keto diet","mediterranean diet","street food","wine recommendations","cocktail recipes","baking tips","meal prep ideas",
  ],

  es: [
    // Noticias & Política
    "noticias de hoy","política argentina","gobierno federal","elecciones 2025","economía nacional","congreso noticias","senado debate","presidente discurso","relaciones exteriores","noticias internacionales","noticias locales","periodismo de investigación","opinión editorial","fact checking",
    // Deportes
    "resultados fútbol","liga española jornada","premier league tabla","champions league sorteo","copa del rey","bundesliga resultados","serie a italia","copa libertadores","copa sudamericana","fórmula 1 carrera","tenis abierto","baloncesto nba","boxeo pelea","mma cartelera","ciclismo vuelta","atletismo mundial","natación competencia","volleyball mundial","rugby six nations","esports torneo","apuestas deportivas","cuotas partido",
    // Finanzas
    "bolsa de valores hoy","precio bitcoin","ethereum cotización","inversiones personales","trading señales","forex análisis","finanzas personales","tasa hipoteca","seguros comparar","jubilación planificación","economía argentina","dólar hoy","euro cotización","inflación datos","tipo de cambio","criptomonedas mercado","startup financiero","capital de riesgo",
    // Tecnología
    "inteligencia artificial noticias","machine learning aplicaciones","ciberseguridad amenazas","startups tecnológicas","desarrollo software","hardware últimos","comparativa smartphones","review iPhone","actualización android","computación en la nube","blockchain noticias","metaverso tendencias","redes sociales tendencias","tiktok viral","instagram reels","youtube creadores","regulación big tech","desarrollo de apps","industria videojuegos",
    // Entretenimiento
    "películas 2025 estrenos","críticas de cine","series de televisión","netflix novedades","streaming plataformas","hbo max series","disney plus estrenos","música chart","conciertos entradas","famosos noticias","premios oscar","grammy nominados","emmy resultados","cultura pop","videos virales","memes hoy","influencers noticias","podcasts recomendados","humor comedy","telenovelas capítulos","reality shows",
    // Lifestyle & Salud
    "recetas saludables","tips cocina","guía restaurantes","dieta keto","nutrición deportiva","rutina fitness","yoga clases","ejercicios gym","salud mental consejos","bienestar hábitos","destinos viaje","ideas vacaciones","hoteles reseñas","viajes de lujo","viaje económico","moda tendencias 2025","belleza tips","rutina skincare","maquillaje tutorial","consejos maternidad","cuidado mascotas","decoración hogar","diseño interior","jardinería tips","proyectos diy","autos eléctricos","review coches",
    // Ciencia & Educación
    "exploración espacial","noticias nasa","cambio climático","noticias medioambiente","investigación médica","tratamiento cáncer","diabetes manejo","noticias vacunas","consejos psicología","descubrimientos biología","avances física","química noticias","conservación océanos","protección fauna","rankings universitarios","cursos en línea","plataformas e-learning",
    // Regionales
    "noticias argentina hoy","noticias méxico hoy","noticias colombia hoy","noticias españa hoy","noticias chile hoy","noticias perú hoy","noticias venezuela hoy","noticias ecuador hoy","noticias uruguay hoy","noticias centroamérica",
    // Negocios & Marketing
    "marketing digital estrategias","seo posicionamiento","marketing de contenidos","redes sociales marketing","email marketing","tendencias ecommerce","dropshipping guía","amazon vendedor","marketing afiliados","negocio online","emprendimiento","pequeñas empresas tips","generación de leads","ideas de negocio","startup latina",
    // Apuestas & Casino
    "apuestas deportivas tips","casino online","estrategia póker","gambling online","carreras caballos","resultados lotería","picks fantasy football","apuestas esports",
    // Automotriz
    "noticias autos","review coches eléctricos","tesla noticias","vehículos autónomos","motos noticias","mantenimiento auto","autos usados","nuevos modelos 2025",
    // Inmobiliario
    "inversión inmobiliaria","noticias propiedades","mercado vivienda","comprar casa tips","alquiler vs compra","inmuebles comerciales","airbnb propietario","calculadora hipoteca",
  ],

  pt: [
    // Notícias & Política
    "notícias de hoje","política brasil","governo federal","eleições 2025","economia nacional","congresso notícias","senado debate","presidente discurso","relações exteriores","notícias internacionais","notícias locais","jornalismo investigativo","opinião editorial",
    // Esportes
    "resultados futebol","campeonato brasileiro tabela","premier league tabela","champions league sorteio","copa do brasil","série a resultados","libertadores","fórmula 1 corrida","tênis aberto","basquete nba","boxe luta","mma card","ciclismo","atletismo mundial","vôlei campeonato","esports torneio","apostas esportivas",
    // Finanças
    "bolsa de valores hoje","preço bitcoin","ethereum cotação","investimentos pessoais","trading sinais","forex análise","finanças pessoais","taxa hipoteca","seguros comparar","previdência planejamento","economia brasileira","dólar hoje","euro cotação","inflação dados","criptomoedas mercado",
    // Tecnologia
    "inteligência artificial notícias","machine learning aplicações","cibersegurança ameaças","startups tecnológicas","desenvolvimento software","hardware últimos","comparativo smartphones","review iPhone","atualização android","computação em nuvem","blockchain notícias","metaverso tendências","redes sociais tendências","tiktok viral","instagram reels","youtube criadores",
    // Entretenimento
    "filmes 2025 lançamentos","críticas cinema","séries televisão","netflix novidades","streaming plataformas","hbo max séries","disney plus lançamentos","música charts","shows ingressos","famosos notícias","prêmios oscar","grammy indicados","cultura pop","vídeos virais","memes hoje","influencers notícias","podcasts recomendados","humor comédia","novelas capítulos",
    // Lifestyle & Saúde
    "receitas saudáveis","dicas culinária","guia restaurantes","dieta low carb","nutrição esportiva","rotina fitness","yoga aulas","exercícios academia","saúde mental dicas","bem-estar hábitos","destinos viagem","ideias férias","hotéis avaliações","viagens luxo","moda tendências 2025","beleza dicas","rotina skincare","maquiagem tutorial","dicas maternidade","cuidado pets","decoração casa","design interior",
    // Regional
    "notícias brasil hoje","notícias portugal hoje","notícias rio de janeiro","notícias são paulo","notícias angola","notícias moçambique",
    // Negócios
    "marketing digital estratégias","seo posicionamento","marketing de conteúdo","e-commerce tendências","empreendedorismo","pequenas empresas dicas","geração de leads","ideias de negócio",
  ],

  it: [
    // Notizie & Politica
    "notizie di oggi","politica italiana","governo notizie","elezioni 2025","economia nazionale","parlamento notizie","senato dibattito","presidente discorso","relazioni estere","notizie internazionali","notizie locali","giornalismo investigativo",
    // Sport
    "risultati calcio","serie a classifica","champions league sorteggio","coppa italia","europa league","formula 1 gara","tennis open","basket nba","boxe incontro","mma card","ciclismo giro","atletica mondiale","nuoto campionato","volley mondiale","esports torneo","scommesse sportive",
    // Finanza
    "borsa oggi","prezzo bitcoin","ethereum quotazione","investimenti personali","trading segnali","forex analisi","finanze personali","tasso mutuo","assicurazioni confronto","pensione pianificazione","economia italiana","euro cambio","inflazione dati","criptovalute mercato",
    // Tecnologia
    "intelligenza artificiale notizie","machine learning applicazioni","cybersecurity minacce","startup tecnologiche","sviluppo software","hardware ultimi","confronto smartphone","review iPhone","aggiornamento android","cloud computing","blockchain notizie","metaverso tendenze","social media tendenze","tiktok viral","instagram reels","youtube creator",
    // Intrattenimento
    "film 2025 uscite","recensioni cinema","serie tv","netflix novità","streaming piattaforme","hbo max serie","disney plus uscite","musica chart","concerti biglietti","famosi notizie","premi oscar","grammy candidati","cultura pop","video virali","meme oggi","influencer notizie","podcast consigliati","commedia show","drama serie",
    // Lifestyle & Salute
    "ricette sane","consigli cucina","guida ristoranti","dieta mediterranea","nutrizione sportiva","routine fitness","yoga lezioni","esercizi palestra","salute mentale consigli","benessere abitudini","destinazioni viaggio","idee vacanze","hotel recensioni","viaggi lusso","moda tendenze 2025","bellezza consigli","routine skincare","trucco tutorial","consigli genitorialità","cura animali","arredamento casa","design interni",
    // Regionale
    "notizie italia oggi","notizie roma","notizie milano","notizie napoli","notizie torino","notizie palermo",
    // Business
    "marketing digitale strategie","seo posizionamento","marketing contenuti","e-commerce tendenze","imprenditorialità","piccole imprese consigli","generazione contatti","idee di business",
  ],

  fr: [
    // Actualités & Politique
    "actualités d'aujourd'hui","politique française","gouvernement actualités","élections 2025","économie nationale","assemblée nationale","sénat débat","président discours","relations internationales","actualités mondiales","actualités locales","journalisme d'investigation",
    // Sport
    "résultats football","ligue 1 classement","champions league tirage","coupe de france","europa league","formule 1 course","tennis open","basket nba","boxe combat","mma card","cyclisme tour","athlétisme mondial","natation championnat","volleyball mondial","esports tournoi","paris sportifs",
    // Finance
    "bourse aujourd'hui","prix bitcoin","ethereum cotation","investissements personnels","trading signaux","forex analyse","finances personnelles","taux hypothèque","assurances comparaison","retraite planification","économie française","euro taux","inflation données","cryptomonnaies marché",
    // Technologie
    "intelligence artificielle actualités","machine learning applications","cybersécurité menaces","startups technologiques","développement logiciel","hardware derniers","comparatif smartphones","review iPhone","mise à jour android","cloud computing","blockchain actualités","métaverse tendances","réseaux sociaux tendances","tiktok viral","instagram reels","youtube créateurs",
    // Divertissement
    "films 2025 sorties","critiques cinéma","séries télévision","netflix nouveautés","streaming plateformes","hbo max séries","disney plus sorties","musique charts","concerts billets","célébrités actualités","oscars 2025","grammy nominés","culture pop","vidéos virales","mèmes aujourd'hui","influenceurs actualités","podcasts recommandés","comédie show","drama série",
    // Lifestyle & Santé
    "recettes saines","conseils cuisine","guide restaurants","régime méditerranéen","nutrition sportive","routine fitness","yoga cours","exercices salle sport","santé mentale conseils","bien-être habitudes","destinations voyage","idées vacances","hôtels avis","voyages luxe","mode tendances 2025","beauté conseils","routine skincare","maquillage tutoriel","conseils parentalité","soin animaux","décoration maison","design intérieur",
    // Régional
    "actualités france aujourd'hui","actualités paris","actualités lyon","actualités marseille","actualités belgique","actualités suisse","actualités québec","actualités afrique francophone",
    // Business
    "marketing digital stratégies","seo référencement","marketing de contenu","e-commerce tendances","entrepreneuriat","petites entreprises conseils","génération de leads","idées de business",
  ],

  ar: [
    // أخبار وسياسة
    "أخبار اليوم","السياسة العربية","الحكومة أخبار","الانتخابات 2025","الاقتصاد الوطني","البرلمان نقاش","الرئيس خطاب","العلاقات الدولية","أخبار عالمية","أخبار محلية","صحافة استقصائية",
    // رياضة
    "نتائج كرة القدم","الدوري السعودي جدول","دوري أبطال أوروبا","كأس العالم 2026","الدوري الإماراتي","فورمولا 1 سباق","تنس أوبن","كرة سلة nba","ملاكمة مباراة","mma بطولة","رياضة عربية","رهانات رياضية",
    // مال واقتصاد
    "البورصة اليوم","سعر البيتكوين","إيثيريوم سعر","استثمار شخصي","تداول إشارات","فوركس تحليل","تمويل شخصي","قرض عقاري","تأمين مقارنة","تقاعد تخطيط","الاقتصاد الخليجي","سعر الدولار","التضخم بيانات","سوق العملات الرقمية",
    // تكنولوجيا
    "الذكاء الاصطناعي أخبار","تعلم الآلة تطبيقات","الأمن السيبراني تهديدات","شركات ناشئة تكنولوجيا","تطوير برمجيات","مراجعة هاتف","مقارنة هواتف ذكية","تطبيقات جديدة","حوسبة سحابية","بلوكتشين أخبار","ميتافيرس","منصات التواصل الاجتماعي","تيك توك فيرال","يوتيوب منشئين",
    // ترفيه وثقافة
    "أفلام 2025 جديدة","مراجعات سينما","مسلسلات جديدة","نتفليكس عربي","منصات بث","موسيقى عربية","حفلات تذاكر","مشاهير أخبار","جوائز سينما","ثقافة عربية","فيديوهات فيرال","مؤثرون أخبار","بودكاست عربي","كوميديا عرض","دراما مسلسل",
    // نمط الحياة والصحة
    "وصفات صحية","نصائح طبخ","دليل مطاعم","نظام غذائي","تغذية رياضية","روتين لياقة","يوغا دروس","تمارين رياضية","صحة نفسية نصائح","رفاهية عادات","وجهات سفر","أفكار عطلة","فنادق مراجعات","سفر فاخر","موضة 2025","جمال نصائح","روتين عناية بشرة","مكياج تعليمي","نصائح تربية","رعاية حيوانات","ديكور منزل","تصميم داخلي",
    // إقليمية
    "أخبار السعودية اليوم","أخبار الإمارات اليوم","أخبار مصر اليوم","أخبار المغرب اليوم","أخبار الكويت","أخبار قطر","أخبار الجزائر","أخبار تونس","أخبار الأردن","أخبار العراق","أخبار لبنان","أخبار ليبيا",
    // أعمال وتسويق
    "تسويق رقمي استراتيجيات","سيو تحسين","تسويق محتوى","تجارة إلكترونية","ريادة أعمال","أعمال صغيرة نصائح","جذب عملاء","أفكار أعمال",
  ],
};

// Mezcla aleatoria (Fisher-Yates) para rotación en cada carga
function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

export function getKeywords(lang = "", search = "") {
  let items = [];
  if (lang && KEYWORDS[lang]) {
    items = shuffle(KEYWORDS[lang]).map(kw => ({ kw, lang }));
  } else {
    // Mezcla inter-idiomas: toma bloques aleatorios de cada idioma
    items = shuffle(
      Object.entries(KEYWORDS).flatMap(([l, kws]) => shuffle(kws).map(kw => ({ kw, lang: l })))
    );
  }
  if (search) {
    const q = search.toLowerCase();
    items = items.filter(k => k.kw.toLowerCase().includes(q));
  }
  return items;
}

export function searchGoogleForDomain(keyword) {
  const query = encodeURIComponent(keyword);
  chrome.tabs.create({ url: `https://www.google.com/search?q=${query}`, active: false });
}
