"""
siard_workflow/core/anonymize/name_dictionary.py

Ordlister over vanlige norske fornavn og etternavn (små bokstaver) brukt til å
GJENKJENNE personnavn-kolonner og personnavn i fritekst deterministisk — uten å
stole på en LLM, som har vist seg upålitelig for nettopp dette (gemma4 svarer
«person» på «J», «Høy», «Klasseliste», «Sokna skole» …).

Listene dekker de vanligste norske navnene (SSB) + utbredte innvandrernavn, men
er ikke uttømmende. Gjenkjenning skjer per token: en verdi regnes som mulig navn
hvis minst ett av ordene finnes her. Kombinert med en form-sjekk
(looks_like_person_name) og «Fornavn Etternavn»-mønster (find_name_spans) gir det
robust skille mot fagnavn, stedsnavn, koder, titler og etiketter.
"""
from __future__ import annotations

# ── Fornavn (gutter/menn + jenter/kvinner) ────────────────────────────────────
_FIRST = """
jan per bjørn bjorn ole lars kjell knut svein geir arne tor odd hans terje morten
rune trond bjarne erik eirik anders andreas martin tomas thomas kristian christian
henrik magnus marius daniel fredrik frederik jonas mathias matias sander jakob
jacob emil oliver william noah lucas lukas filip philip phillip aksel oskar oscar
tobias elias håkon hakon sondre sigurd eivind espen frode roar stein vidar egil
leif reidar harald olav nils gunnar ivar rolf johan jon ola petter pål pal are
atle audun bård bard dag finn gaute halvard jørgen jorgen jens kåre kare ketil kim
lasse ludvig nikolai nicolai ørjan orjan øystein oystein øyvind oyvind rasmus
sebastian simen sivert steffen stian sverre truls vegard viktor victor jim tommy
roger ronny kenneth glenn robert richard rikard george john michael mikael david
peter paul mark james christoffer kristoffer kasper casper alf amund benjamin
bernt birger borgar brage brede carl didrik eilif endre erlend even gard gjermund
gustav halvor herman isak iver joakim johannes josef markus mats nikolaj olaus
ottar ragnar roald selmer sigmund sindre snorre syver teodor torgeir tormod
trygve ulrik vebjørn vetle wilhelm åge age åsmund asmund adrian alexander
aleksander aron arthur august edvard edvin elliot emanuel fabian felix gabriel
georg gisle hallvard håvard havard ingar jarle jesper joachim jostein kevin
kjetil kristen leander leon levi loke ludvik magne marcus marko mikkel mons
morgan natanael niklas nikolas oddmund odin patrick patrik preben ragnvald robin
severin sigve simon sjur stefan sten stig storm sven thor thorbjørn thorvald tom
torbjørn tore torfinn torkil torstein tryggve ulf valter waldemar yngve zakarias
ahmed muhammad muhammed mohammed mohammad ali hassan hussein ibrahim omar yusuf
yousef abdi said mehmet abdullah amir aram dawit habib hamid karim khaled mahmoud
mustafa nasir rashid samir tariq amin bilal farid imran sami sanjay ravi raj
kumar amar deepak jose juan carlos pedro miguel antonio luis pablo marco andrei
piotr pawel jakub tomasz michal krzysztof grzegorz mateusz kacper artem dmitri
sergei viktor oleksandr andriy osama dawoud
anne inger kari marit ingrid liv eva berit hilde bente anita nina marianne solveig
randi tone astrid sigrid hanne else gerd turid ida emma nora sofie sofia sara saga
maja maya thea julie linnea tuva ingeborg frida hanna hannah amalie olivia ella
leah aurora mia ada selma vilde kristin camilla silje maria mari elisabeth heidi
wenche wencke tove grete unni britt ellen karin kaja lene linda monica cathrine
catrine trine ann mette kjersti gunn aud reidun bjørg bjorg eli may frøydis froydis
evy belinda lill ragnhild gro torill marte marthe ingvild oda emilie malin sunniva
hedda ingunn vivian elin guri hege janne jorunn kristine laila lise maren mona
ragna rikke ruth signe siri synne tonje vibeke åse ase agnes alma andrea ane
benedikte birgit cecilie celine dagny dorthe edel gina helene henriette iren irene
johanne jenny katrine lea live lotte louise magnhild mathilde nadia natalie nelly
oline ronja rose sandra stine susanne tina vanja victoria viktoria wilma yvonne
aase adele agnete alexandra aleksandra alida alva amanda anette annette antonia
ariel astri åshild ashild beate bodil borghild brita charlotte christine dina dorte
ebba eline elise erle ester esther fatima frøya froya gunhild guro hedvig helga
henny hilda hjørdis idun iselin jane jeanette jorid juni karine karoline kine
kirsten kjellaug klara linn lisbeth lone magda margit margrete nanna oddny olava
pernille petra rakel rannveig rita runa sigrun siv solfrid sonja svanhild synnøve
synnove tirill tiril torunn unni-marie vendela vera veronika veronica åsa asa
amina aisha fatma layla mariam khadija noor zainab sara-marie nadia leila yasmin molly
""".split() + """
aage abel adam alvar anton arent aslak asle atle audun balder bastian birk bjarte
bjornar brede brynjar cato christer cornelius dagfinn didrik eberhard edgar edmund
eilif eindride einar eldar elmer elvin embret emrik endre engebret erland esten
fartein folke fritjof gabriel gard geirmund gjert gorm gudbrand gudmund gunder
gunnstein haldor halsten hartvig helmer hjalmar holger idar inge ingvald isak ivan
jarand jardar jarl jentoft joar johnny jonatan jostein jorund kai karsten kasper
kjartan klaus kolbein konrad kornelius laurits leiv lennart levin ludvig luka mads
magnar magnor malvin manfred martinus melvin mikal njaal noa nikolas odvar olai
olaus olve orm ottar ove peder pelle pontus preben ragnvald raymond reidulf remi
rikard roald roland rolv ruben sakarias salomon samson severin sigbjorn sigfred
sivert snorre staale sturle styrk syver teodor thorleif thorstein tinius tollef
tonny tord toralf torleif torolf tron uno valdemar vebjorn verner vetle viggo
viljar villads waldemar wiggo wilmer yngvar zacharias hermod borgar torgny aslag
oddvar arnstein arnfinn arnt asbjorn audunn baard birger bjornulf brattvoll
dankert eilert elling erland fridtjof gisle hallstein haugen hauk havar hilmar
ingmar isaak jarle jasper joachim johannes jorund knud kornel kristofer ludvik
mariusz mikael mons normann oddbjorn olaf oluf osvald rasmus rikhard roar
sigvald simen sjur stale steinar svale sven-erik tarjei thomas tollak torfinn
torgny torstein ulrik vegar vemund vidkun villum vilmar yngvar
aagot aase adelheid agda agnar alfhild alma alvhild anlaug annbjorg annlaug arnhild
asbjorg aslaug asta augusta bergljot bjorghild bodil borgny brynhild dagrun dordi
dorthea ebba edith eldbjorg elfrid ellinor embla erna esther fanny fredrikke gerda
gjertrud gladys gudny gunnvor halldis hedvig helga helle herborg hjordis ingebjorg
ingfrid ingun jenny jorunn josefine kaia karen kirsti kjellfrid klara kristine
laura laurine liss live magda magnhild margit marta mathea mette milla mina nanna
nelly nora nikoline oddlaug oddrun oddveig olaug olga ovidia petrine ragna ragnfrid
rakel reidun rikke saima signy sigrunn solbjorg solgunn svanaug sylvi tanja tora
torbjorg torgunn turi vendela vigdis vilhelmine wenche ynglinga oda solfrid randi
gunvor anny borghild dagmar gunnbjorg ingeborg jensine karoline ludvikka magda
marie nikolina otilie petrikke ragnhild sofie torhild valborg vesla aagoth
aleksandra anneli barbro berit-anne dorthea elise eva-marie frida gunhild hildegunn
ingrid-marie jorid kjellrun liv-marit malene mari-anne oline ragnhild-marie
sigrid-marie thea-marie tonje
johan gustav nils sven axel folke hampus hugo love melker theodor valter alva elsa
freja greta linnea maja stina kerstin lena ulrika harald carsten jeppe lasse lone
pernille birgitte clara sofie aleksi antti eero hannu jari jukka juha matti mika
pekka sami tapio teemu timo tuomas ville aino anni eeva elina katri leena marja
minna paivi riitta sari tiina ulla bjarni gudmundur olafur sigurdur stefan gudrun
kristin sigridur
abdirahman abdullahi abdirisak abdul akram amir anwar asif bashir faisal farhan
hamza imran ismail jamal javed kamran khaled khalid mahad mahmoud nadeem nasir
osman rashid saad salah samir shahid tariq usman waqar yusuf zeeshan halima sumaya
hodan ayaan fadumo deeqa hanan najma sahra warsame mukhtar omar said hassan hussein
aleksander artur bartosz dawid jakub kamil krzysztof lukasz marcin mateusz michal
pawel piotr rafal sebastian szymon tomasz wojciech adrian dominik agnieszka
katarzyna magdalena malgorzata
""".split()


# ── Etternavn ─────────────────────────────────────────────────────────────────
_LAST = """
hansen johansen olsen larsen andersen pedersen nilsen kristiansen jensen karlsen
johnsen pettersen eriksen berg haugen hagen johannessen andreassen jacobsen
jakobsen dahl jørgensen jorgensen halvorsen lund moen iversen strand solberg bakke
moe lie holm aas myhre nguyen johansson knutsen martinsen ali gundersen holmen
sørensen sorensen henriksen antonsen mikkelsen nygård nygard bakken næss ness ruud
kristoffersen amundsen danielsen kaspersen stømne stomne robstad sørlie sorlie sand
bø bo li ødegård odegard fredriksen abrahamsen aune arnesen birkeland borg brekke
bredesen christiansen edvardsen ellingsen engebretsen evensen fossum gulbrandsen
hauge haugland helland hovland isaksen jenssen kalland kleven knudsen kolstad
kvam lunde madsen mathisen melby mo myhren nilssen nordby nordli olaussen paulsen
rasmussen rønning ronning rødland rodland samuelsen sivertsen skaug skjelbred skogen
steen sundby svendsen syvertsen thomassen thoresen torgersen ulriksen vik viken
wold ås østby ostby aamodt aarseth aasen abelsen aronsen arvesen backe bang barstad
berge berntsen birkelund bjørnstad bjornstad blix bøe bratberg braathen breivik
brenna buer bugge carlsen christophersen corneliussen davidsen dalen drabløs egeland
egge eide eikeland eilertsen einarsen ekeberg eliassen ellefsen elstad emberland
endresen engen engelsen enoksen erichsen estensen fagerli fagerheim falch farstad
finstad fjeld fjellheim foss frantzen fredheim furset gabrielsen gjerde gjertsen
granli grimstad grønli gronli gulliksen gustavsen haldorsen halse hammer hammerstad
hanssen harila helgesen hermansen hetland hoel hoff hofstad holberg holen holt
holter hovde huseby håland haaland ingebrigtsen ingvaldsen jansen johnsrud jonassen
juel kalstad kittelsen kjær kjos klausen kleppe knausen koren kristensen krogh
kvalheim lange langeland lien lillevik lindberg lindstrøm lindstrom løvås lovaas
madssen martinussen mathiassen meland mella mellem mæland nakken nesheim nordahl
nordberg nordheim nordgård norheim nybø nyhus olafsen olerud opdal opsahl ottesen
ovesen pålsen palsen persen ramberg ramstad randen reinertsen rian risberg rise
roald rognlien rønneberg rosendal rud ryen sagen sandberg sande sandnes sæther
saether tangen tellefsen thorbjørnsen thorsen thune tjelle tobiassen tollefsen
tønnessen tonnessen torp tveit ueland ulset undheim utne vaage valle vang vatne
vold volden waaler wang wik wilhelmsen winther wold zachariassen aune solbakken
solem solheim sollie solvang stang stene stenseth stokke strøm strom strømme stromme
sundal sunde sveen tangen khan ahmad sheikh hussain rahman begum chowdhury islam
kaur singh shah patel yilmaz demir kaya celik öztürk ozturk silva santos garcia
fernandez lopez gonzalez kowalski nowak wisniewski wojcik kaminski petrov ivanov
sokolov müller muller schmidt schneider fischer weber
""".split() + """
aalberg aamodt aarsland aasebo aasen abelseth aaheim aakvik aamot aarset aasland
aasmundsen aaberge agdestein alme almvik amundrud andersson andresen arntsen arntzen
asphaug aspaas aspelund austad austbo aukland baardsen baardsrud bakka bakkebo
bakkehaug bakker bakketun barlund bekkelund benjaminsen berentsen berge berget
berggreen berglund bergstrom bergvik birkenes bjellum bjerke bjerkeli bjornland
bjorndal bjornerud bjornevik blakstad bleie blindheim bolstad borgen borgersen
botnen braten brattbakk brattland breivoll bremnes brenne brevik brubakk bruflot
brustad bryne buan buene buvik byberg dalseth davidsen edland eikrem eilefsen
ekeland ekeli elden eldegard elnes elveland endresen enerud engan engebakken
engelstad engevik enstad erdal erstad espeland evjen evju fagerbakke fagerland
falkenberg farsund fauske fedje fjeldstad fjellanger fjellstad flatland flem flo
floysand foldnes forberg fosse fossen fossmo framnes fredly froland furnes gangstad
gausdal gilje gjelsvik gjerstad gjessing glomnes godtland granmo gravdal greni grini
grimsrud gronning grude gulliksrud gunnerud gusdal gystad haga hagland halland
hammerseng hansteen harberg harestad hatlen hauan haugdal haugnes hauknes haver
heggdal heggem hellevik helmersen hemnes herstad heggebo hjeltnes hodne holand
holden holdhus holstad holsether hopland huse husby hustad hustveit isene joakimsen
johansrud joeng jordal juvik kaldheim karlstad kavli kile kirkeng kjelsrud kjorstad
klakegg klepp klingenberg kolnes krakstad kroken krokstad krossli kvaale kvalsund
kvande kvarme kvinge laksesvela landmark langset lervik lerstad liabo lieng lillebo
lindgaard lindheim listou loland loftheim lohne lokken lome lothe lundberg lundby
lunder lyngstad madslangrud melhus mellgren midthun midtbo moberg modalen molund
myrvang naas nakling naustdal nesbo nesvold njaastad nordbo nybakk nydal nylund odden
oddstad oksnes olderbakk omdal opheim oppedal orset osland osmundsen ostbo ostgard
ostvik overland pahr pareli paulshus pedersrud rakkestad rana randen rasdal raustol
reigstad reines remlo rian rimol risnes risto rodland roen rognan rongved rosseland
rovde rygg ryland sagvolden salbu salvesen sandbakk sannes sather selnes setsaas
siljan simonsen sjursen skaalvik skaane skarbo skarpnes skarsbo skattum skeie skjeret
skjervheim skogheim skomedal skoresen sletten sletvold smebye snekkevik solbakk
solbu solem solhaug solli soltveit somdal sortland staurset steensnaes stensland
storaker storeng storvoll strommen sundvor svalheim svardal svardstad svelmo svingen
swang saetervik soreng soreide tafjord taraldsen teigland tessem thommessen
thorbjornsrud thunes tjora tofte tonnesen torsvik tortveit tronstad trygstad tveiten
tysse ueland uglem utgard vaagen vagle valland varhaug vassbo veastad veimo vestby
vestli vethe vidnes vigeland viland vingen vistnes volle vollan westgaard wiik
windju yri ystad oygarden ovrebo aakre aamelfot aarvik
eriksson johansson karlsson nilsson larsson olsson persson svensson gustafsson
lindqvist lindgren forsberg bergman sjoberg wallin engstrom danielsson hakansson
lundin gunnarsson jonsson petersson lundgren mattsson lundqvist nystrom lindholm
lofgren fransson palm sjostrom isaksson nielsen christensen poulsen moller mortensen
korhonen virtanen makinen nieminen makela hamalainen laine heikkinen koskinen
jarvinen lehtonen lehtinen saarinen salminen heinonen niemi heikkila kinnunen
salonen turunen salo laitinen tuominen rantanen karjalainen jokinen mattila
gudmundsson sigurdsson jonsson stefansson palsson gunnarsson einarsson
""".split()


FIRST_NAMES = frozenset(_FIRST)
LAST_NAMES = frozenset(_LAST)
ALL_NAMES = FIRST_NAMES | LAST_NAMES

# Fornavn som også er vanlige norske ord/forkortelser. Disse skal IKKE utløse
# treff ALENE i fritekst (gir falske treff som «F1.Dag», «per stk», «odd»), men
# fanges fortsatt i fulle navn der et etternavn følger («Dag Hansen», «Per Berg»).
AMBIGUOUS_FIRST = frozenset("""
dag even odd liv mai may gro bent storm ravn ask vår var are mons finn per pér
tor stein sten bjørn bjorn alf ask brage gard vilde rose roar mali siv tone
love bo bror live saga embla birk orm tron luka noa folke inge mons love junior
balder gard orm tora turi siv live bo love folke holger hugo love melker valter
""".split())


def is_known_name_token(word: str) -> bool:
    """True hvis ordet er et kjent norsk fornavn eller etternavn."""
    return (word or "").strip(".'-").lower() in ALL_NAMES
