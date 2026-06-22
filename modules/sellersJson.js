// ============================================================
// ADEQ Toolbar — sellers.json import
// ------------------------------------------------------------
// Cada empresa publisher/intermediary publica /sellers.json (estándar IAB)
// listando todos los sitios con los que trabaja. Es una mina de oro de leads:
// scrape el JSON, filtra los seller_type=PUBLISHER, encolá en csv_queue.
// ============================================================

// Lista baked-in de empresas conocidas. El user puede agregar más
// (se persisten en chrome.storage.local + Supabase toolbar_config).
// URLs verificadas 2026-05-11 (pubs = cantidad de seller_type=PUBLISHER al momento del check).
// Ordenadas por cantidad de publishers — los más grandes primero.
export const DEFAULT_SELLERS_COMPANIES = [
  { name: "improvedigital.com", url: "https://improvedigital.com/sellers.json" },
  { name: "truvid.com", url: "https://www.truvid.com/sellers.json" },
  { name: "themoneytizer.com", url: "https://www.themoneytizer.com/sellers.json" },
  { name: "triplelift.com", url: "https://triplelift.com/sellers.json" },
  { name: "vidoomy.com", url: "https://www.vidoomy.com/sellers.json" },
  { name: "teads.tv", url: "https://teads.tv/sellers.json" },
  { name: "pubmatic.com", url: "https://pubmatic.com/sellers.json" },
  { name: "ad.plus", url: "https://ad.plus/sellers.json" },
  { name: "openx.com", url: "https://openx.com/sellers.json" },
  { name: "sharethrough.com", url: "https://sharethrough.com/sellers.json" },
  { name: "optad360.com", url: "https://optad360.com/sellers.json" },
  { name: "setupad.com", url: "https://setupad.com/sellers.json" },
  { name: "indexexchange.com", url: "https://www.indexexchange.com/sellers.json" },
  { name: "152media.info", url: "https://152media.info/sellers.json" },
  { name: "mowplayer.com", url: "https://mowplayer.com/sellers.json" },
  { name: "nsightvideo.com", url: "https://nsightvideo.com/sellers.json" },
  { name: "rubiconproject.com", url: "https://rubiconproject.com/sellers.json" },
  { name: "smartadserver.com", url: "https://smartadserver.com/sellers.json" },
  { name: "seedtag.com", url: "https://www.seedtag.com/sellers.json" },
  { name: "verve.com", url: "https://verve.com/sellers.json" },
  { name: "revcontent.com", url: "https://revcontent.com/sellers.json" },
  { name: "mgid.com", url: "https://mgid.com/sellers.json" },
  { name: "propellerads.com", url: "https://propellerads.com/sellers.json" },
  { name: "admaven.com", url: "https://admaven.com/sellers.json" },
  { name: "equativ.com", url: "https://equativ.com/sellers.json" },
  { name: "adagio.io", url: "https://adagio.io/sellers.json" },
  { name: "showheroes.com", url: "https://showheroes.com/sellers.json" },
  { name: "adyoulike.com", url: "https://adyoulike.com/sellers.json" },
  { name: "smartclip.com", url: "https://smartclip.com/sellers.json" },
  { name: "yieldbird.com", url: "https://yieldbird.com/sellers.json" },
  { name: "aniview.com", url: "https://aniview.com/sellers.json" },
  { name: "anyclip.com", url: "https://anyclip.com/sellers.json" },
  { name: "vidazoo.com", url: "https://vidazoo.com/sellers.json" },
  { name: "openweb.com", url: "https://openweb.com/sellers.json" },
  { name: "mobfox.com", url: "https://mobfox.com/sellers.json" },
  { name: "adtelligent.com", url: "https://adtelligent.com/sellers.json" },
  { name: "adkernel.com", url: "https://adkernel.com/sellers.json" },
  { name: "aax.network", url: "https://aax.network/sellers.json" },
  { name: "taboola.com", url: "https://taboola.com/sellers.json" },
  { name: "outbrain.com", url: "https://outbrain.com/sellers.json" },
  { name: "nativo.com", url: "https://nativo.com/sellers.json" },
  { name: "applovin.com", url: "https://applovin.com/sellers.json" },
  { name: "ironsrc.com", url: "https://ironsrc.com/sellers.json" },
  { name: "inmobi.com", url: "https://inmobi.com/sellers.json" },
  { name: "smaato.com", url: "https://smaato.com/sellers.json" },
  { name: "mintegral.com", url: "https://mintegral.com/sellers.json" },
  { name: "chartboost.com", url: "https://chartboost.com/sellers.json" },
  { name: "digitalturbine.com", url: "https://digitalturbine.com/sellers.json" },
  { name: "buysellads.com", url: "https://buysellads.com/sellers.json" },
  { name: "gourmetads.com", url: "https://gourmetads.com/sellers.json" },
  { name: "e-planning.net", url: "https://e-planning.net/sellers.json" },
  { name: "mediavine.com", url: "https://mediavine.com/sellers.json" },
  { name: "adthrive.com", url: "https://adthrive.com/sellers.json" },
  { name: "monetizemore.com", url: "https://monetizemore.com/sellers.json" },
  { name: "nitropay.com", url: "https://nitropay.com/sellers.json" },
  { name: "snack-media.com", url: "https://snack-media.com/sellers.json" },
  { name: "freestar.io", url: "https://freestar.io/sellers.json" },
  { name: "playwire.com", url: "https://playwire.com/sellers.json" },
  { name: "publift.com", url: "https://publift.com/sellers.json" },
  { name: "underdogmedia.com", url: "https://underdogmedia.com/sellers.json" },
  { name: "venatus.com", url: "https://venatus.com/sellers.json" },
  { name: "sunmedia.tv", url: "https://sunmedia.tv/sellers.json" },
  { name: "adsmovil.com", url: "https://adsmovil.com/sellers.json" },
  { name: "connatix.com", url: "https://connatix.com/sellers.json" },
  { name: "glomex.com", url: "https://glomex.com/sellers.json" },
  { name: "primis.tech", url: "https://primis.tech/sellers.json" },
  { name: "unrulymedia.com", url: "https://unrulymedia.com/sellers.json" },
  { name: "reklamstore.com", url: "https://www.reklamstore.com/sellers.json" },
  { name: "static.cdn.admatic.com.tr", url: "https://static.cdn.admatic.com.tr/sellers/sellers.json" },
  { name: "unity.com", url: "https://unity.com/sellers.json" },
  { name: "loopme.com", url: "https://loopme.com/sellers.json" },
  { name: "pubgalaxy.com", url: "https://pubgalaxy.com/sellers.json" },
  { name: "premiumads.com.br", url: "https://premiumads.com.br/sellers.json" },
  { name: "projectagora.com", url: "https://projectagora.com/sellers.json" },
  { name: "smartframe.io", url: "https://smartframe.io/sellers.json" },
  { name: "simpleads.com.br", url: "https://simpleads.com.br/sellers.json" },
  { name: "criteo.com", url: "https://criteo.com/sellers.json" },
  { name: "adwmg.com", url: "https://adwmg.com/sellers.json" },
  { name: "holid.io", url: "https://holid.io/sellers.json" },
  { name: "freewheel.com", url: "https://freewheel.com/sellers.json" },
  { name: "adpone.com", url: "https://adpone.com/sellers.json" },
  { name: "dazn.com", url: "https://www.dazn.com/sellers.json" },
  { name: "adhese.com", url: "https://adhese.com/sellers.json" },
  { name: "adnimation.com", url: "https://adnimation.com/sellers.json" },
  { name: "richaudience.com", url: "https://richaudience.com/sellers.json" },
  { name: "exte.com", url: "https://exte.com/sellers.json" },
  { name: "adops.gr", url: "https://adops.gr/sellers.json" },
  { name: "ogury.com", url: "https://ogury.com/sellers.json" },
  { name: "vistarmedia.com", url: "https://vistarmedia.com/sellers.json" },
  { name: "movingup.it", url: "https://movingup.it/sellers.json" },
  { name: "odeeo.io", url: "https://odeeo.io/sellers.json" },
  { name: "evolutionadv.it", url: "https://evolutionadv.it/sellers.json" },
  { name: "footballco.com", url: "https://footballco.com/sellers.json" },
  { name: "optidigital.com", url: "https://optidigital.com/sellers.json" },
  { name: "audienzz.ch", url: "https://audienzz.ch/sellers.json" },
  { name: "clickio.com", url: "https://clickio.com/sellers.json" },
  { name: "r2b2.cz", url: "https://r2b2.cz/sellers.json" },
  { name: "broadsign.com", url: "https://broadsign.com/sellers.json" },
  { name: "harrenmedia.com", url: "https://harrenmedia.com/sellers.json" },
  { name: "streamlyn.com", url: "https://streamlyn.com/sellers.json" },
  { name: "webads.nl", url: "https://webads.nl/sellers.json" },
  { name: "onlinemediasolutions.com", url: "https://onlinemediasolutions.com/sellers.json" },
  { name: "phaistosnetworks.gr", url: "https://www.phaistosnetworks.gr/sellers.json" },
  { name: "strossle.com", url: "https://strossle.com/sellers.json" },
  { name: "tappx.com", url: "https://tappx.com/sellers.json" },
  { name: "dianomi.com", url: "https://dianomi.com/sellers.json" },
  { name: "stroeer.com", url: "https://stroeer.com/sellers.json" },
  { name: "joinads.me", url: "https://joinads.me/sellers.json" },
  { name: "adsocy.com", url: "https://adsocy.com/sellers.json" },
  { name: "soundstack.com", url: "https://soundstack.com/sellers.json" },
  { name: "voisetech.com", url: "https://voisetech.com/sellers.json" },
  { name: "adweb.gr", url: "https://adweb.gr/sellers.json" },
  { name: "yoc.com", url: "https://yoc.com/sellers.json" },
  { name: "onetag.com", url: "https://onetag.com/sellers.json" },
  { name: "refinery89.com", url: "https://refinery89.com/sellers.json" },
  { name: "adswizz.com", url: "https://adswizz.com/sellers.json" },
  { name: "alkimi.org", url: "https://alkimi.org/sellers.json" },
  { name: "amagi.com", url: "https://amagi.com/sellers.json" },
  { name: "overwolf.com", url: "https://overwolf.com/sellers.json" },
  { name: "iion.io", url: "https://iion.io/sellers.json" },
  { name: "adform.com", url: "https://adform.com/sellers.json" },
  { name: "connectad.io", url: "https://connectad.io/sellers.json" },
  { name: "entravision.com", url: "https://entravision.com/sellers.json" },
  { name: "brightcom.com", url: "https://brightcom.com/sellers.json" },
  { name: "yieldlove.com", url: "https://yieldlove.com/sellers.json" },
  { name: "wurl.com", url: "https://wurl.com/sellers.json" },
  { name: "alright.com.br", url: "https://alright.com.br/sellers.json" },
  { name: "otzads.net", url: "https://otzads.net/sellers.json" },
  { name: "stroeer.de", url: "https://stroeer.de/sellers.json" },
  { name: "digohispanicmedia.com", url: "https://digohispanicmedia.com/sellers.json" },
  { name: "seznam.cz", url: "https://seznam.cz/sellers.json" },
  { name: "flower-ads.com", url: "https://flower-ads.com/sellers.json" },
  { name: "adverty.com", url: "https://adverty.com/sellers.json" },
  { name: "undertone.com", url: "https://undertone.com/sellers.json" },
  { name: "adtonos.com", url: "https://adtonos.com/sellers.json" },
  { name: "adasta.it", url: "https://adasta.it/sellers.json" },
  { name: "mediasquare.fr", url: "https://mediasquare.fr/sellers.json" },
  { name: "gazeta.pl", url: "https://gazeta.pl/sellers.json" },
  { name: "contentignite.com", url: "https://contentignite.com/sellers.json" },
  { name: "iprom.si", url: "https://iprom.si/sellers.json" },
  { name: "yieldlab.net", url: "https://yieldlab.net/sellers.json" },
  { name: "sapo.pt", url: "https://sapo.pt/sellers.json" },
  { name: "next14.com", url: "https://next14.com/sellers.json" },
  { name: "massarius.com", url: "https://massarius.com/sellers.json" },
  { name: "ividence.com", url: "https://ividence.com/sellers.json" },
  { name: "ozoneproject.com", url: "https://ozoneproject.com/sellers.json" },
  { name: "wp.pl", url: "https://wp.pl/sellers.json" },
  { name: "livewrapped.com", url: "https://livewrapped.com/sellers.json" },
  { name: "relevant-digital.com", url: "https://relevant-digital.com/sellers.json" },
  { name: "beintoo.com", url: "https://beintoo.com/sellers.json" },
  { name: "cpex.cz", url: "https://cpex.cz/sellers.json" },
  { name: "rakuten.tv", url: "https://www.rakuten.tv/sellers.json" },
  { name: "ringier-advertising.ch", url: "https://ringier-advertising.ch/sellers.json" },
  { name: "hubvisor.io", url: "https://hubvisor.io/sellers.json" },
  { name: "russmedia.com", url: "https://russmedia.com/sellers.json" },
  { name: "adtarget.biz", url: "https://adtarget.biz/sellers.json" },
  { name: "sibboventures.com", url: "https://sibboventures.com/sellers.json" },
  { name: "newixmedia.com", url: "https://newixmedia.com/sellers.json" },
  { name: "arbomedia.ro", url: "https://arbomedia.ro/sellers.json" },
  { name: "prismamedia.com", url: "https://prismamedia.com/sellers.json" },
  { name: "smartstream.tv", url: "https://smartstream.tv/sellers.json" },
  { name: "adssets.com", url: "https://adssets.com/sellers.json" },
  { name: "proximus.be", url: "https://proximus.be/sellers.json" },
  { name: "manzoniadvertising.it", url: "https://manzoniadvertising.it/sellers.json" },
  { name: "bluebillywig.com", url: "https://bluebillywig.com/sellers.json" },
  { name: "italiaonline.it", url: "https://italiaonline.it/sellers.json" },
  { name: "366.fr", url: "https://366.fr/sellers.json" },
  { name: "ad-alliance.de", url: "https://ad-alliance.de/sellers.json" },
  { name: "produpress.be", url: "https://produpress.be/sellers.json" },
  { name: "canelamedia.com", url: "https://canelamedia.com/sellers.json" },
  { name: "emetriq.com", url: "https://emetriq.com/sellers.json" },
  { name: "wemass.com", url: "https://wemass.com/sellers.json" },
  { name: "nativery.com", url: "https://cdn.nativery.com/widget/js/sellers.json" },
  { name: "pebblemedia.be", url: "https://pebblemedia.be/sellers.json" },
  { name: "adsanddata.be", url: "https://adsanddata.be/sellers.json" },
  { name: "seven.one", url: "https://seven.one/sellers.json" },
  { name: "onedio.com", url: "https://onedio.com/sellers.json" },
  { name: "first-id.fr", url: "https://first-id.fr/sellers.json" },
  { name: "logan.ai", url: "https://logan.ai/sellers.json" },
  { name: "bonniernews.se", url: "https://bonniernews.se/sellers.json" },
  { name: "mediamond.it", url: "https://www.mediamond.it/sellers.json" },
  { name: "admoai.com", url: "https://admoai.com/sellers.json" },
  { name: "dpgmedia.be", url: "https://dpgmedia.be/sellers.json" },
  { name: "sabah.com.tr", url: "https://sabah.com.tr/sellers.json" },
  { name: "invidi.com", url: "https://invidi.com/sellers.json" },
  { name: "impresa.pt", url: "https://impresa.pt/sellers.json" },
  { name: "samsung.com", url: "https://www.samsung.com/sellers.json" },
  { name: "rmb.be", url: "https://rmb.be/sellers.json" },
  { name: "dexerto.com", url: "https://dexerto.com/sellers.json" },
  { name: "i-mobile.co.jp", url: "https://i-mobile.co.jp/sellers.json" },
  { name: "ucfunnel.com", url: "https://ucfunnel.com/sellers.json" },
  { name: "aralego.com", url: "https://aralego.com/sellers.json" },
  { name: "auxoads.com", url: "https://auxoads.com/sellers.json" },
  { name: "greedygame.com", url: "https://greedygame.com/sellers.json" },
  { name: "vertoz.com", url: "https://vertoz.com/sellers.json" },
  { name: "adop.cc", url: "https://adop.cc/sellers.json" },
  { name: "ad-stir.com", url: "https://ad-stir.com/sellers.json" },
  { name: "microad.co.jp", url: "https://microad.co.jp/sellers.json" },
  { name: "ad-generation.jp", url: "https://ad-generation.jp/sellers.json" },
  { name: "vdo.ai", url: "https://vdo.ai/sellers.json" },
  { name: "adingo.jp", url: "https://adingo.jp/sellers.json" },
  { name: "yeahmobi.com", url: "https://yeahmobi.com/sellers.json" },
  { name: "innity.com", url: "https://innity.com/sellers.json" },
  { name: "playstream.media", url: "https://playstream.media/sellers.json" },
  { name: "fout.jp", url: "https://fout.jp/sellers.json" },
  { name: "momagic.com", url: "https://momagic.com/sellers.json" },
  { name: "admicro.vn", url: "https://admicro.vn/sellers.json" },
  { name: "pokkt.com", url: "https://pokkt.com/sellers.json" },
  { name: "xapads.com", url: "https://xapads.com/sellers.json" },
  { name: "logly.co.jp", url: "https://logly.co.jp/sellers.json" },
  { name: "adgebra.co", url: "https://adgebra.co/sellers.json" },
  { name: "vuukle.com", url: "https://vuukle.com/sellers.json" },
  { name: "adpopcorn.com", url: "https://adpopcorn.com/sellers.json" },
  { name: "geniee-ssp.net", url: "https://geniee-ssp.net/sellers.json" },
  { name: "playground.xyz", url: "https://playground.xyz/sellers.json" },
  { name: "vidcrunch.com", url: "https://vidcrunch.com/sellers.json" },
  { name: "adview.com", url: "https://adview.com/sellers.json" },
  { name: "adpushup.com", url: "https://adpushup.com/sellers.json" },
  { name: "zmaticoo.com", url: "https://zmaticoo.com/sellers.json" },
  { name: "foxpush.com", url: "https://foxpush.com/sellers.json" },
  { name: "adintop.com", url: "https://adintop.com/sellers.json" },
  { name: "arabyads.com", url: "https://arabyads.com/sellers.json" },
  { name: "adlive.io", url: "https://adlive.io/sellers.json" },
  { name: "andbeyond.media", url: "https://andbeyond.media/sellers.json" },
  { name: "dochase.com", url: "https://dochase.com/sellers.json" },
  { name: "kueez.com", url: "https://kueez.com/sellers.json" },
  { name: "kueezrtb.com", url: "https://kueezrtb.com/sellers.json" },
  { name: "mobupps.com", url: "https://mobupps.com/sellers.json" },
  { name: "wortise.com", url: "https://wortise.com/sellers.json" },
  { name: "denakop.com", url: "https://denakop.com/sellers.json" },
  { name: "nobeta.com.br", url: "https://nobeta.com.br/sellers.json" },
  { name: "audienciad.com", url: "https://audienciad.com/sellers.json" },
  { name: "membrana.media", url: "https://membrana.media/sellers.json" },
  { name: "juicebarads.com", url: "https://juicebarads.com/sellers.json" },
  { name: "adzep.com.br", url: "https://adzep.com.br/sellers.json" },
  { name: "grumft.com", url: "https://grumft.com/sellers.json" },
  { name: "rtbhouse.com", url: "https://rtbhouse.com/sellers.json" },
  { name: "4wmarketplace.com", url: "https://4wmarketplace.com/sellers.json" },
  { name: "sublime.xyz", url: "https://sublime.xyz/sellers.json" },
  { name: "justpremium.com", url: "https://justpremium.com/sellers.json" },
  { name: "quantum-advertising.com", url: "https://quantum-advertising.com/sellers.json" },
  { name: "cwire.com", url: "https://cwire.com/sellers.json" },
  { name: "bidmachine.io", url: "https://bidmachine.io/sellers.json" },
  { name: "insticator.com", url: "https://insticator.com/sellers.json" },
  { name: "adapex.io", url: "https://adapex.io/sellers.json" },
  { name: "sevio.com", url: "https://sevio.com/sellers.json" },
  { name: "stailamedia.com", url: "https://stailamedia.com/sellers.json" },
  { name: "betweendigital.com", url: "https://betweendigital.com/sellers.json" },
  { name: "smartclip.net", url: "https://smartclip.net/sellers.json" },
  { name: "madvertise.com", url: "https://madvertise.com/sellers.json" },
  { name: "adverline.com", url: "https://adverline.com/sellers.json" },
  { name: "admixer.net", url: "https://admixer.net/sellers.json" },
  { name: "admixer.com", url: "https://admixer.com/sellers.json" },
  { name: "eskimi.com", url: "https://eskimi.com/sellers.json" },
  { name: "nativery.com", url: "https://nativery.com/sellers.json" },
  { name: "venatusmedia.com", url: "https://venatusmedia.com/sellers.json" },
  { name: "themediagrid.com", url: "https://themediagrid.com/sellers.json" },
  { name: "targetspot.com", url: "https://www.targetspot.com/sellers.json" },
  { name: "33across.com", url: "https://33across.com/sellers.json" },
  { name: "media.net", url: "https://media.net/sellers.json" },
  { name: "appnexus.com", url: "https://appnexus.com/sellers.json" },
  { name: "conversantmedia.com", url: "https://conversantmedia.com/sellers.json" },
  { name: "adcolony.com", url: "https://adcolony.com/sellers.json" },
  { name: "fyber.com", url: "https://fyber.com/sellers.json" },
  { name: "nextmillennium.io", url: "https://nextmillennium.io/sellers.json" },
  { name: "lkqd.com", url: "https://lkqd.com/sellers.json" },
  { name: "minutemedia.com", url: "https://minutemedia.com/sellers.json" },
  { name: "gumgum.com", url: "https://gumgum.com/sellers.json" },
  { name: "yieldmo.com", url: "https://yieldmo.com/sellers.json" },
  { name: "beachfront.com", url: "https://beachfront.com/sellers.json" },
  { name: "kargo.com", url: "https://kargo.com/sellers.json" },
  { name: "brid.tv", url: "https://brid.tv/sellers.json" },
  { name: "sonobi.com", url: "https://sonobi.com/sellers.json" },
  { name: "publir.com", url: "https://publir.com/sellers.json" },
  { name: "springserve.com", url: "https://springserve.com/sellers.json" },
  { name: "smartyads.com", url: "https://smartyads.com/sellers.json" },
  { name: "onomagic.com", url: "https://onomagic.com/sellers.json" },
  { name: "pulsepoint.com", url: "https://pulsepoint.com/sellers.json" },
  { name: "adsparc.com", url: "https://adsparc.com/sellers.json" },
  { name: "pixfuture.com", url: "https://pixfuture.com/sellers.json" },
  { name: "adsyield.com", url: "https://adsyield.com/sellers.json" },
  { name: "mediafuse.com", url: "https://mediafuse.com/sellers.json" },
];

// Check si los dominios ya fueron procesados antes (csv_queue + review_queue + historial).
// Devuelve Set de dominios ya conocidos. Útil para no re-encolar leads que ya pasamos.
export async function findKnownDomains(supabaseUrl, anonKey, accessToken, candidates, opts = {}) {
  if (!Array.isArray(candidates) || candidates.length === 0) return new Set();
  const known = new Set();
  const headers = { "apikey": anonKey, "Authorization": `Bearer ${accessToken}` };
  const BATCH = 200;
  // Maxi 2026-06-18: opts.mode controla qué tablas chequea:
  //  - "all" (default): csv_queue + review_queue + historial + sendtrack + blocklist
  //  - "monday_refresh": solo csv_queue activo + blocklist
  //    (uso desde "IMPORT MONDAY BOARD WEBSITES" → Ciclo Finalizado = querés
  //     re-prospectar aunque tenga sendtrack/historial viejo)
  let tables;
  if (opts.mode === "monday_refresh") {
    tables = [
      // Solo csv_queue activo (pending/processing) — evita duplicar job actual
      { table: "toolbar_csv_queue",     col: "domain", filter: "&status=in.(pending,processing,waiting_pool)" },
      // Blocklist sí — no re-procesar dominios bloqueados aunque vengan de Monday
      { table: "toolbar_url_blocklist", col: "domain", filter: "" },
    ];
  } else {
    // COBERTURA PROGRESIVA (Maxi 2026-06-22): un import de sellers/CSV descarta el
    // dominio si YA pasó por el sistema alguna vez — cualquier fila en la cola
    // (cualquier status: pending/processing/waiting/done/skipped/frozen/next_day) o
    // cualquier fila en Prospects. Así el import recorre el JSON SIN repetir: cada
    // tanda toma URLs nuevas, quedan marcadas, y la siguiente avanza a las que faltan.
    // Cuando ya no quedan nuevas, "ya analizados" es REAL (ese partner está agotado).
    // Para RE-PROSPECTAR finalizados se usa el flujo Monday (mode monday_refresh), que
    // NO mira esta historia. Tras un reset total (cola vacía) todo vuelve a ser nuevo.
    tables = [
      { table: "toolbar_csv_queue",     col: "domain", filter: "" },
      { table: "toolbar_review_queue",  col: "domain", filter: "" },
    ];
  }
  for (let i = 0; i < candidates.length; i += BATCH) {
    const slice = candidates.slice(i, i + BATCH);
    const inList = slice.map(d => `"${d.replace(/"/g, '\\"')}"`).join(",");
    await Promise.all(tables.map(async ({ table, col, filter }) => {
      try {
        const res = await fetch(
          `${supabaseUrl}/rest/v1/${table}?${col}=in.(${encodeURIComponent(inList)})&select=${col}${filter || ""}`,
          { headers }
        );
        if (!res.ok) return;
        const rows = await res.json();
        rows.forEach(r => {
          const v = r[col];
          if (typeof v === "string" && v) known.add(v.toLowerCase());
        });
      } catch {}
    }));
  }
  return known;
}

// Empresas verificadas SIN sellers.json público (al 2026-05-11):
//   - Ezoic       (https://www.ezoic.com/sellers.json → 404)
//   - The Monetizer (themonetizer.com → 404)
//   - Playvid360  (DNS no resuelve)
// Si conseguís la URL real, agregalo desde el botón ✏️ Edit.

// Fetch sellers.json + extrae solo los PUBLISHER (skip INTERMEDIARY/BOTH del owner).
// Retorna lista de dominios deduplicados y normalizados.
export async function fetchSellersJson(url) {
  // SIN headers Accept — algunos servidores rechazan preflight CORS si lo mandamos.
  // Timeout 30s para sellers.json grandes (Truvid 689KB, Vidoomy 592KB).
  let res;
  try {
    res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: AbortSignal.timeout(30000),
    });
  } catch (e) {
    // Errores de red comunes: TypeError "Failed to fetch" (CORS), DNS, timeout.
    throw new Error(`Network error: ${e.message || e.name}. ${e.name === "TimeoutError" ? "Timeout 30s." : "Posible CORS/DNS/SSL — el servidor puede estar bloqueando fetch desde extensions."}`);
  }
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch { throw new Error(`Response no es JSON válido (¿HTML 404 o respuesta binaria?)`); }
  const sellers = Array.isArray(data?.sellers) ? data.sellers : [];
  // Filtro flexible: acepta PUBLISHER en cualquier capitalización ("Publisher", "publisher").
  // INTERMEDIARY/BOTH del owner se descartan.
  const domains = sellers
    .filter(s => (s.seller_type || "").toUpperCase() === "PUBLISHER")
    .map(s => normalizeDomain(s.domain || ""))
    .filter(Boolean);
  // Dedupe
  return [...new Set(domains)];
}

function normalizeDomain(d) {
  if (!d || typeof d !== "string") return "";
  return d.toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/.*$/, "")
    .trim();
}

// Parsea el ads.txt de un sitio y devuelve dominios únicos de ad systems.
// Formato ads.txt: "domain.com, accountId, RELATIONSHIP, certAuthority"
// Útil para descubrir nuevas empresas con sellers.json.
export async function fetchAdsTxtSystems(siteUrl) {
  const url = (() => {
    try { return new URL("/ads.txt", siteUrl).href; } catch { return null; }
  })();
  if (!url) throw new Error("URL inválida");
  const res = await fetch(url, {
    method: "GET",
    redirect: "follow",
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} en ${url}`);
  const text = await res.text();
  // HTML check (404 page que devuelve 200)
  if (/^\s*<!doctype|<html/i.test(text.trim())) {
    throw new Error("Respuesta no es ads.txt (parece HTML 404)");
  }
  const systems = new Set();
  text.split("\n").forEach(line => {
    const clean = line.split("#")[0].trim();
    if (!clean) return;
    const parts = clean.split(",").map(s => s.trim());
    if (parts.length < 3) return;
    const domain = normalizeDomain(parts[0]);
    // Skip subdomain-style ads.txt (CNAME/subdomain= que no son ad systems)
    if (!domain || /^(subdomain|contact|cname)/i.test(domain)) return;
    systems.add(domain);
  });
  return [...systems].sort();
}

// Probe en paralelo: para cada dominio ad-system, prueba si /sellers.json existe
// y devuelve {domain, url, pubs}. Skip los que fallan.
// Concurrency limitada para no quemar la red (8 simultáneos).
export async function probeSellersJson(domains, onProgress) {
  const results = [];
  const CONCURRENCY = 8;
  let completed = 0;
  const probe = async (domain) => {
    const tryUrls = [
      `https://${domain}/sellers.json`,
      `https://www.${domain}/sellers.json`,
    ];
    for (const url of tryUrls) {
      try {
        const res = await fetch(url, {
          method: "GET", redirect: "follow",
          signal: AbortSignal.timeout(8000),
          headers: { "Accept": "application/json" },
        });
        if (!res.ok) continue;
        const text = await res.text();
        if (/^\s*<!doctype|<html/i.test(text.trim())) continue;
        let data;
        try { data = JSON.parse(text); } catch { continue; }
        const pubs = (data?.sellers || []).filter(s => (s.seller_type || "").toUpperCase() === "PUBLISHER").length;
        if (pubs > 0) return { domain, url: res.url || url, pubs };
      } catch {}
    }
    return null;
  };
  // Worker pool
  const queue = [...domains];
  const workers = Array(Math.min(CONCURRENCY, queue.length)).fill(0).map(async () => {
    while (queue.length > 0) {
      const domain = queue.shift();
      if (!domain) break;
      const r = await probe(domain);
      completed++;
      if (onProgress) onProgress(completed, domains.length, r);
      if (r) results.push(r);
    }
  });
  await Promise.all(workers);
  results.sort((a, b) => b.pubs - a.pubs);
  return results;
}
