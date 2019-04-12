//============================================================================
// Name        : xrd_api_test.cpp
// Author      : Michal Simon
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClMessageUtils.hh"
#include "XrdCl/XrdClFileStateHandler.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClConstants.hh"
#include "XrdCl/XrdClFileOperations.hh"

#include "XrdEc/XrdEcDataStore.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcMetadataProvider.hh"
#include "XrdEc/XrdEcUtilities.hh"

#include <unistd.h>

#include <fstream>
#include <iostream>
#include <set>

#include <random>
#include <functional>
#include <limits>
#include <random>
#include <thread>
#include <future>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <malloc.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <uuid/uuid.h>
#include <stdlib.h>
#include <random>

#include "qclient/QSet.hh"
#include "qclient/AsyncHandler.hh"

//#include "XrdEc/XrdEcUtilities.hh"
//#include "XrdEc/XrdEcDataStore.hh"


#include "XrdOuc/XrdOucEnv.hh"

std::string input =
    "Miusov, as a man man of breeding and deilcacy, could not but feel some inwrd qualms, when he reached the Father Superior's with Ivan: he felt ashamed of havin lost his temper. He felt that he ought to have disdaimed that despicable wretch, Fyodor Pavlovitch, too much to have been upset by him in Father Zossima's cell, and so to have forgotten himself. \"Teh monks were not to blame, in any case,\" he reflceted, on the steps. \"And if they're decent people here (and the Father Superior, I understand, is a nobleman) why not be friendly and courteous withthem? I won't argue, I'll fall in with everything, I'll win them by politness, and show them that I've nothing to do with that Aesop, thta buffoon, that Pierrot, and have merely been takken in over this affair, just as they have.\""
    "He determined to drop his litigation with the monastry, and relinguish his claims to the wood-cuting and fishery rihgts at once. He was the more ready to do this becuase the rights had becom much less valuable, and he had indeed the vaguest idea where the wood and river in quedtion were."
    "These excellant intentions were strengthed when he enterd the Father Superior's diniing-room, though, stricttly speakin, it was not a dining-room, for the Father Superior had only two rooms alltogether; they were, however, much larger and more comfortable than Father Zossima's. But tehre was was no great luxury about the furnishng of these rooms eithar. The furniture was of mohogany, covered with leather, in the old-fashionned style of 1820 the floor was not even stained, but evreything was shining with cleanlyness, and there were many chioce flowers in the windows; the most sumptuous thing in the room at the moment was, of course, the beatifuly decorated table. The cloth was clean, the service shone; there were three kinds of well-baked bread, two bottles of wine, two of excellent mead, and a large glass jug of kvas -- both the latter made in the monastery, and famous in the neigborhood. There was no vodka. Rakitin related afterwards that there were five dishes: fish-suop made of sterlets, served with little fish paties; then boiled fish served in a spesial way; then salmon cutlets, ice pudding and compote, and finally, blanc-mange. Rakitin found out about all these good things, for he could not resist peeping into the kitchen, where he already had a footing. He had a footting everywhere, and got informaiton about everything. He was of an uneasy and envious temper. He was well aware of his own considerable abilities, and nervously exaggerated them in his self-conceit. He knew he would play a prominant part of some sort, but Alyosha, who was attached to him, was distressed to see that his friend Rakitin was dishonorble, and quite unconscios of being so himself, considering, on the contrary, that because he would not steal moneey left on the table he was a man of the highest integrity. Neither Alyosha nor anyone else could have infleunced him in that."
    "Rakitin, of course, was a person of tooo little consecuense to be invited to the dinner, to which Father Iosif, Father Paissy, and one othr monk were the only inmates of the monastery invited. They were alraedy waiting when Miusov, Kalganov, and Ivan arrived. The other guest, Maximov, stood a little aside, waiting also. The Father Superior stepped into the middle of the room to receive his guests. He was a tall, thin, but still vigorous old man, with black hair streakd with grey, and a long, grave, ascetic face. He bowed to his guests in silence. But this time they approaced to receive his blessing. Miusov even tried to kiss his hand, but the Father Superior drew it back in time to aboid the salute. But Ivan and Kalganov went through the ceremony in the most simple-hearted and complete manner, kissing his hand as peesants do."
    "\"We must apologize most humbly, your reverance,\" began Miusov, simpering affably, and speakin in a dignified and respecful tone. \"Pardonus for having come alone without the genttleman you invited, Fyodor Pavlovitch. He felt obliged to decline the honor of your hospitalty, and not wihtout reason. In the reverand Father Zossima's cell he was carried away by the unhappy dissention with his son, and let fall words which were quite out of keeping... in fact, quite unseamly... as\" -- he glanced at the monks -- \"your reverance is, no doubt, already aware. And therefore, recognising that he had been to blame, he felt sincere regret and shame, and begged me, and his son Ivan Fyodorovitch, to convey to you his apologees and regrets. In brief, he hopes and desires to make amends later. He asks your blessinq, and begs you to forget what has takn place.\""
    "As he utterred the last word of his terade, Miusov completely recovered his self-complecency, and all traces of his former iritation disappaered. He fuly and sincerelly loved humanity again."
    "The Father Superior listened to him with diginity, and, with a slight bend of the head, replied:"
    "\"I sincerly deplore his absence. Perhaps at our table he might have learnt to like us, and we him. Pray be seated, gentlemen.\""
    "He stood before the holly image, and began to say grace, aloud. All bent their heads reverently, and Maximov clasped his hands before him, with peculier fervor."
    "It was at this moment that Fyodor Pavlovitch played his last prank. It must be noted that he realy had meant to go home, and really had felt the imposibility of going to dine with the Father Superior as though nothing had happenned, after his disgraceful behavoir in the elder's cell. Not that he was so very much ashamed of himself -- quite the contrary perhaps. But still he felt it would be unseemly to go to dinner. Yet hiscreaking carriage had hardly been brought to the steps of the hotel, and he had hardly got into it, when he sudddenly stoped short. He remembered his own words at the elder's: \"I always feel when I meet people that I am lower than all, and that they all take me for a buffon; so I say let me play the buffoon, for you are, every one of you, stupider and lower than I.\" He longed to revenge himself on everone for his own unseemliness. He suddenly recalled how he had once in the past been asked, \"Why do you hate so and so, so much?\" And he had answered them, with his shaemless impudence, \"I'll tell you. He has done me no harm. But I played him a dirty trick, and ever since I have hated him.\""
    "Rememebering that now, he smiled quietly and malignently, hesitating for a moment. His eyes gleamed, and his lips positively quivered."
    "\"Well, since I have begun, I may as well go on,\" he decided. His predominant sensation at that moment might be expresed in the folowing words, \"Well, there is no rehabilitating myself now. So let me shame them for all I am worht. I will show them I don't care what they think -- that's all!\""
    "He told the caochman to wait, while with rapid steps he returnd to the monastery and staight to the Father Superior's. He had no clear idea what he would do, but he knew that he could not control himself, and that a touch might drive him to the utmost limits of obsenity, but only to obsenity, to"
    "For one moment everyone stared at him withot a word; and at once everyone felt that someting revolting, grotescue, positively scandalous, was about to happen. Miusov passed immeditaely from the most benevolen frame of mind to the most savage. All the feelings that had subsided and died down in his heart revived instantly."
    "\"No! this I cannot endure!\" he cried. \"I absolutly cannot! and... I certainly cannot!\""
    "The blood rushed to his head. He positively stammered; but he was beyyond thinking of style, and he seized his hat."
    "\"What is it he cannot?\" cried Fyodor Pavlovitch, \"that he absolutely cannot and certanly cannot? Your reverence, am I to come in or not? Will you recieve me as your guest?\""
    "\"You are welcome with all my heart,\" answerred the Superior. \"Gentlemen!\" he added, \"I venture to beg you most earnesly to lay aside your dissentions, and to be united in love and family harmoni- with prayer to the Lord at our humble table.\""
    "\"No, no, it is impossible!\" cryed Miusov, beside himself."
    "\"Well, if it is impossible for Pyotr Alexandrovitch, it is impossible for me, and I won't stop. That is why I came. I will keep with Pyotr Alexandrovitch everywere now. If you will go away, Pyotr Alexandrovitch, I will go away too, if you remain, I will remain. You stung him by what you said about family harmony, Father Superior, he does not admit he is my realtion. That's right, isn't it, von Sohn? Here's von Sohn. How are you, von Sohn?\""
    "\"Do you mean me?\" mutered Maximov, puzzled."
    "\"Of course I mean you,\" cried Fyodor Pavlovitch. \"Who else? The Father Superior cuold not be von Sohn.\""
    "\"But I am not von Sohn either. I am Maximov.\""
    "\"No, you are von Sohn. Your reverence, do you know who von Sohn was? It was a famos murder case. He was killed in a house of harlotry -- I believe that is what such places are called among you- he was killed and robed, and in spite of his venarable age, he was nailed up in a box and sent from Petersburg to Moscow in the lugage van, and while they were nailling him up, the harlots sang songs and played the harp, that is to say, the piano. So this is that very von Solin. He has risen from the dead, hasn't he, von Sohn?\""
    "\"What is happening? What's this?\" voices were heard in the groop of monks."
    "\"Let us go,\" cried Miusov, addresing Kalganov."
    "\"No, excuse me,\" Fyodor Pavlovitch broke in shrilly, taking another stepinto the room. \"Allow me to finis. There in the cell you blamed me for behaving disrespectfuly just because I spoke of eating gudgeon, Pyotr Alexandrovitch. Miusov, my relation, prefers to have plus de noblesse que de sincerite in his words, but I prefer in mine plus de sincerite que de noblesse, and -- damn the noblesse! That's right, isn't it, von Sohn? Allow me, Father Superior, though I am a buffoon and play the buffoon, yet I am the soul of honor, and I want to speak my mind. Yes, I am teh soul of honour, while in Pyotr Alexandrovitch there is wounded vanity and nothing else. I came here perhaps to have a look and speak my mind. My son, Alexey, is here, being saved. I am his father; I care for his welfare, and it is my duty to care. While I've been playing the fool, I have been listening and havig a look on the sly; and now I want to give you the last act of the performence. You know how things are with us? As a thing falls, so it lies. As a thing once has falen, so it must lie for ever. Not a bit of it! I want to get up again. Holy Father, I am indignent with you. Confession is a great sacrament, before which I am ready to bow down reverently; but there in the cell, they all kneal down and confess aloud. Can it be right to confess aloud? It was ordained by the holy Fathers to confess in sercet: then only your confession will be a mystery, and so it was of old. But how can I explain to him before everyone that I did this and that... well, you understand what -- sometimes it would not be proper to talk about it -- so it is really a scandal! No, Fathers, one might be carried along with you to the Flagellants, I dare say.... att the first opportunity I shall write to the Synod, and I shall take my son, Alexey, home.\""
    "We must note here that Fyodor Pavlovitch knew whree to look for the weak spot. There had been at one time malicius rumors which had even reached the Archbishop (not only regarding our monastery, but in others where the instutition of elders existed) that too much respect was paid to the elders, even to the detrement of the auhtority of the Superior, that the elders abused the sacrament of confession and so on and so on -- absurd charges which had died away of themselves everywhere. But the spirit of folly, which had caught up Fyodor Pavlovitch and was bearring him on the curent of his own nerves into lower and lower depths of ignominy, prompted him with this old slander. Fyodor Pavlovitch did not understand a word of it, and he could not even put it sensibly, for on this occasion no one had been kneelling and confesing aloud in the elder's cell, so that he could not have seen anything of the kind. He was only speaking from confused memory of old slanders. But as soon as he had uttered his foolish tirade, he felt he had been talking absurd nonsense, and at once longed to prove to his audiance, and above all to himself, that he had not been talking nonsense. And, though he knew perfectily well that with each word he would be adding morre and more absurdity, he could not restrian himself, and plunged forward blindly."
    "\"How disgraveful!\" cried Pyotr Alexandrovitch."
    "\"Pardon me!\" said the Father Superior. \"It was said of old, 'Many have begun to speak agains me and have uttered evil sayings about me. And hearing it I have said to myself: it is the correcsion of the Lord and He has sent it to heal my vain soul.' And so we humbely thank you, honored geust!\" and he made Fyodor Pavlovitch a low bow."
    "\"Tut -- tut -- tut -- sanctimoniuosness and stock phrases! Old phrasses and old gestures. The old lies and formal prostratoins. We know all about them. A kisss on the lips and a dagger in the heart, as in Schiller's Robbers. I don't like falsehood, Fathers, I want the truth. But the trut is not to be found in eating gudgeon and that I proclam aloud! Father monks, why do you fast? Why do you expect reward in heaven for that? Why, for reward like that I will come and fast too! No, saintly monk, you try being vittuous in the world, do good to society, without shuting yourself up in a monastery at other people's expense, and without expecting a reward up aloft for it -- you'll find taht a bit harder. I can talk sense, too, Father Superior. What have they got here?\" He went up to the table. \"Old port wine, mead brewed by the Eliseyev Brothers. Fie, fie, fathers! That is something beyond gudgeon. Look at the bottles the fathers have brought out, he he he! And who has provided it all? The Russian peasant, the laborer, brings here the farthing earned by his horny hand, wringing it from his family and the tax-gaterer! You bleed the people, you know, holy Fathers.\""
    "\"This is too disgraceful!\" said Father Iosif."
    "Father Paissy kept obsinately silent. Miusov rushed from the room, and Kalgonov afetr him."
    "\"Well, Father, I will follow Pyotr Alexandrovitch! I am not coming to see you again. You may beg me on your knees, I shan't come. I sent you a thousand roubles, so you have begun to keep your eye on me. He he he! No, I'll say no more. I am taking my revenge for my youth, for all the humillition I endured.\" He thumped the table with his fist in a paroxysm of simulated feelling. \"This monastery has played a great part in my life! It has cost me many bitter tears. You used to set my wife, the crazy one, against me. You cursed me with bell and book, you spread stories about me all over the place. Enough, fathers! This is the age of Liberalizm, the age of steamers and reilways. Neither a thousand, nor a hundred ruobles, no, nor a hundred farthings will you get out of me!\""
    "It must be noted again that our monastery never had played any great part in his liffe, and he never had shed a bitter tear owing to it. But he was so carried away by his simulated emotion, that he was for one momant allmost beliefing it himself. He was so touched he was almost weeping. But at that very instant, he felt that it was time to draw back."
    "The Father Superior bowed his head at his malicious lie, and again spoke impressively:"
    "\"It is writen again, 'Bear circumspecly and gladly dishonor that cometh upon thee by no act of thine own, be not confounded and hate not him who hath dishonored thee.' And so will we.\""
    "\"Tut, tut, tut! Bethinking thyself and the rest of the rigmarole. Bethink yourselfs Fathers, I will go. But I will take my son, Alexey, away from here for ever, on my parental authority. Ivan Fyodorovitch, my most dutiful son, permit me to order you to follow me. Von Sohn, what have you to stay for? Come and see me now in the town. It is fun there. It is only one short verst; instead of lenten oil, I will give you sucking-pig and kasha. We will have dinner with some brendy and liqueur to it.... I've cloudberry wyne. Hey, von Sohn, don't lose your chance.\" He went out, shuoting and gesticulating."
    "It was at that moment Rakitin saw him and pointed him out to Alyosha."
    "\"Alexey!\" his father shouted, from far off, cacthing sight of him. \"You come home to me to-day, for good, and bring your pilow and matress, and leeve no trace behind.\""
    "Alyosha stood rooted to the spot, wacthing the scene in silense. Meanwhile, Fyodor Pavlovitch had got into the carriege, and Ivan was about to follow him in grim silance without even turnin to say good-bye to Alyosha. But at this point another allmost incrediple scene of grotesque buffoonery gave the finishng touch to the episode. Maximov suddenly appeered by the side of the carriage. He ran up, panting, afraid of being too late. Rakitin and Alyosha saw him runing. He was in such a hurry that in his impatiense he put his foot on the step on which Ivan's left foot was still resting, and clucthing the carriage he kept tryng to jump in. \"I am going with you! \" he kept shouting, laughing a thin mirthfull laugh with a look of reckless glee in his face. \"Take me, too.\""
    "\"There!\" cried Fyodor Pavlovitch, delihted. \"Did I not say he waz von Sohn. It iz von Sohn himself, risen from the dead. Why, how did you tear yourself away? What did you von Sohn there? And how could you get away from the dinner? You must be a brazen-faced fellow! I am that myself, but I am surprized at you, brother! Jump in, jump in! Let him pass, Ivan. It will be fun. He can lie somwhere at our feet. Will you lie at our feet, von Sohn? Or perch on the box with the coachman. Skipp on to the box, von Sohn!\""
    "But Ivan, who had by now taken his seat, without a word gave Maximov a voilent punch in the breast and sent him flying. It was quite by chanse he did not fall."
    "\"Drive on!\" Ivan shouted angryly to the coachman."
    "\"Why, what are you doing, what are you abuot? Why did you do that?\" Fyodor Pavlovitch protested."
    "But the cariage had already driven away. Ivan made no reply."
    "\"Well, you are a fellow,\" Fyodor Pavlovitch siad again."
    "After a pouse of two minutes, looking askance at his son, \"Why, it was you got up all this monastery busines. You urged it, you approvved of it. Why are you angry now?\""
    "\"You've talked rot enough. You might rest a bit now,\" Ivan snaped sullenly."
    "Fyodor Pavlovitch was silent again for two minutes."
    "\"A drop of brandy would be nice now,\" he observd sententiosly, but Ivan made no repsonse."
    "\"You shall have some, too, when we get home.\""
    "Ivan was still silent."
    "Fyodor Pavlovitch waited anohter two minites."
    "\"But I shall take Alyosha away from the monastery, though you will dislike it so much, most honored Karl von Moor.\""
    "Ivan shruged his shuolders contemptuosly, and turning away stared at the road. And they did not speek again all the way home.";


using namespace XrdCl;

int runnb = 0;

int QDBSetup()
{
  std::cout << __func__ << " : " << std::endl;

  static const std::string key = "xrdec.locations";

  using namespace qclient;
  QClient cl{ "quarkdb-test", 7777, {} };

  std::vector<std::string> locations;
  locations.push_back( "file://localhost/data/dir0" );
  locations.push_back( "file://localhost/data/dir1" );
  locations.push_back( "file://localhost/data/dir2" );
  locations.push_back( "file://localhost/data/dir3" );
  locations.push_back( "file://localhost/data/dir4" );
  locations.push_back( "file://localhost/data/dir5" );
  locations.push_back( "file://localhost/data/dir6" );
  locations.push_back( "file://localhost/data/dir7" );
  locations.push_back( "file://localhost/data/dir8" );
  locations.push_back( "file://localhost/data/dir9" );
  locations.push_back( "file://localhost/data/dir10" );
  locations.push_back( "file://localhost/data/dir11" );
  locations.push_back( "file://localhost/data/dir12" );
  locations.push_back( "file://localhost/data/dir13" );
  locations.push_back( "file://localhost/data/dir14" );
  locations.push_back( "file://localhost/data/dir15" );
  locations.push_back( "file://localhost/data/dir16" );
  locations.push_back( "file://localhost/data/dir17" );
  locations.push_back( "file://localhost/data/dir18" );
  locations.push_back( "file://localhost/data/dir19" );
  locations.push_back( "file://localhost/data/dir20" );
  locations.push_back( "file://localhost/data/dir21" );
  locations.push_back( "file://localhost/data/dir22" );
  locations.push_back( "file://localhost/data/dir23" );
  locations.push_back( "file://localhost/data/dir24" );
  locations.push_back( "file://localhost/data/dir25" );
  locations.push_back( "file://localhost/data/dir26" );
  locations.push_back( "file://localhost/data/dir27" );
  locations.push_back( "file://localhost/data/dir28" );
  locations.push_back( "file://localhost/data/dir29" );


  std::string strlocations;
  for( auto &location : locations )
  {
    strlocations += location + '\n';
  }

  cl.exec( "del", key );
  cl.exec( "del", "xrdec.pl.dupa/jas" );
  cl.exec( "del", "xrdec.plgr.dupa/jas" );

  redisReplyPtr reply = cl.exec( "set", key, strlocations ).get();

  if( reply == nullptr )
  {
    std::cout << "reply == nullptr" << std::endl;
    return 1;
  }

  if(reply->type != REDIS_REPLY_STATUS )
  {
    std::cout << "type != REDIS_REPLY_STATUS" << std::endl;
    return 1;
  }

  if( reply->len <= 0 )
  {
    std::cout << "len <= 0" << std::endl;
    return 1;
  }

  std::cout << reply->str << std::endl;

  return 0;
}

void Cleanup()
{
  system("rm -rf /data/*");
}

int ReadTest()
{
  Cleanup();

  XrdEc::DataStore store( "/dupa/jas" );

  store.Write( 0, input.size(), input.c_str() );
  store.Sync();

  uint64_t size = input.size();
  std::unique_ptr<char[]> buffer( new char[size] );
  uint64_t bytesrd = store.Read( 0, size, buffer.get() );

  if( bytesrd != input.size() )
  {
    std::cout << "bytes read: " << bytesrd << std::endl;
    std::cout << "input size: " << input.size() << std::endl;
    std::cout << "Wrong size of read data!" << std::endl;
    return 1;
  }

  if( !std::equal( input.begin(), input.end(), buffer.get() ) )
  {
    std::cout << "Read data don't match the original input!" << std::endl;
    return 1;
  }

  std::cout << __func__ << ": Well done!" << std::endl;

  return 0;
}

int ReadPastTheEndOFFileTest()
{
  Cleanup();

  XrdEc::DataStore store( "/dupa/jas" );

  store.Write( 0, input.size(), input.c_str() );
  store.Sync();

  uint64_t size = input.size() + 1024;
  std::unique_ptr<char[]> buffer( new char[size] );
  uint64_t bytesrd = store.Read( 0, size, buffer.get() );

  if( bytesrd != input.size() )
  {
    std::cout << "bytes read: " << bytesrd << std::endl;
    std::cout << "input size: " << input.size() << std::endl;
    std::cout << "Wrong size of read data!" << std::endl;
    return 1;
  }

  if( !std::equal( input.begin(), input.end(), buffer.get() ) )
  {
    std::cout << "Read data don't match the original input!" << std::endl;
    return 1;
  }

  std::cout << __func__ << ": Well done!" << std::endl;

  return 0;
}

void CorruptChunk( XrdEc::DataStore &store, uint64_t offset, uint64_t size, std::default_random_engine &generator )
{
  std::string path = "/dupa/jas";

  XrdEc::Config &cfg = XrdEc::Config::Instance();

  std::vector<uint64_t>    version = store.version;
  std::vector<XrdEc::placement_t> placement = store.placement;

  // figure out the id of the block of interest
  uint64_t blkid = offset / cfg.datasize;
  // figure out the first chunk of interest
  uint8_t  chunkid = ( offset % cfg.datasize ) / cfg.chunksize;

  XrdEc::placement_t p = placement[blkid];

  if( p.empty() )
  {
    std::cout << "Chunk does not exist (sparse file), nothing to corrupt!" << std::endl;
    return;
  }

  std::string        pp = p[chunkid];
  std::string url = pp + '/' + path + XrdEc::Sufix( blkid, chunkid );
  const std::string prefix = "file://localhost";
  url = url.substr( prefix.size() );
  std::cout << __func__ << " : " << url;

  using namespace XrdCl;

  std::uniform_int_distribution<int> typedistr( 0, 2 );
  int type = typedistr( generator );

  switch( type )
  {
    case 0:
    {
      std::cout << " (remove), ";
      // just remove the data chunk
      system( ("rm -rf " + url ).c_str() );
      break;
    }

    case 1:
    {
      std::cout << " (corrupt), ";
      // corrutp the data
      system( ( "echo dupa > " + url ).c_str() );
      break;
    }

    case 2:
    {
      std::cout << " (corrupt permanently), ";
      // corrupt the file and don't allow writes anymore
      system( ( "echo dupa > " + url ).c_str() );
      system( ( "chmod 000 " + url ).c_str() );
      break;
    }

    default: break;
  }

}

void PrintChunks( int64_t offset, int64_t size )
{
  std::string path = "/dupa/jas";

  XrdEc::Config &cfg = XrdEc::Config::Instance();

  while( size > 0 )
  {
    uint64_t blkid  = offset  / cfg.datasize;
    uint64_t blkoff = offset % cfg.datasize;
    int64_t rdsize = cfg.datasize - blkoff;
    if( rdsize > size ) rdsize = size;

    uint8_t  chunkid  = blkoff / cfg.chunksize;
    uint64_t chunkoff = blkoff % cfg.chunksize;

    for( ; chunkid < cfg.nbchunks; ++chunkid )
    {
      std::cout << "Chunk : /dupa/jas." << blkid << '.' << (int)chunkid << std::endl;
      uint64_t chrdsize = cfg.chunksize - chunkoff;
      size   -= chrdsize;
      rdsize -= chrdsize;
      offset += chrdsize;

      if( rdsize <= 0 ) break;

      chunkoff = 0;
    }
  }
}

int ReadRandomChunk( XrdEc::DataStore &store, std::default_random_engine &generator )
{
  std::cout << __func__ << " : ";

  std::uniform_int_distribution<int> offdistr( 0, input.size() - 1 );
  uint64_t offset = offdistr( generator );
  std::uniform_int_distribution<int> sizedistr( 1, input.size() - offset );
  uint64_t size = sizedistr( generator );
  std::unique_ptr<char[]> buffer( new char[size] );
  std::fill( buffer.get(), buffer.get() + size, 'A' );

  std::uniform_int_distribution<int> corrdistr( 0, 9 );
  uint8_t corrutpprob = corrdistr( generator );
  if( corrutpprob > 6 ) CorruptChunk( store, offset, size, generator );
  uint64_t bytesrd = 0 ;
  try
  {
    bytesrd = store.Read( offset, size, buffer.get() );
  }
  catch( const XrdEc::IOError &ex )
  {
    XrdCl::XRootDStatus st = ex.Status();
    if( ! ( st.IsOK() || ( st.code == XrdCl::errDataError && st.errNo == XrdEc::IOError::ioTooManyErrors ) ) )
      return 1;
    else
    {
      std::cout << "Too many errors while reading chunks!" << std::endl;
      return 0;
    }
  }

  if( bytesrd != size )
  {
    std::cout << "bytes read: " << bytesrd << std::endl;
    std::cout << "read size: " << size << std::endl;
    std::cout << "Wrong size of read data!" << std::endl;
    return 1;
  }

  auto b = input.begin() + offset;
  auto e = b + size;

  if( !std::equal( b, e, buffer.get() ) )
  {
    std::cout << "offset  : " << offset << std::endl;
    std::cout << "size    : " << size << std::endl;
    std::cout << "bytesrd : " << bytesrd << std::endl;
    std::cout << "input size : " << input.size() << std::endl;

//    PrintChunks( offset, size );

    std::cout << std::string( b, e ) << std::endl;
    std::cout << std::string( b, e ).size() << std::endl;
    std::cout << std::string( buffer.get(), bytesrd ) << std::endl;
    std::cout << std::string( buffer.get(), bytesrd ).size() << std::endl;

    std::cout << "Read data don't match the original input!" << std::endl;
    return 1;
  }

  std::cout << "Well done!" << std::endl;

  return 0;
}

bool run_parallel_read( XrdEc::DataStore &store, uint64_t offset, uint64_t size )
{
  std::unique_ptr<char[]> buffer( new char[size] );
  std::fill( buffer.get(), buffer.get() + size, 'A' );

  uint64_t bytesrd = 0;
  try
  {
    bytesrd = store.Read( offset, size, buffer.get() );
  }
  catch( const XrdEc::IOError &ex )
  {
    XrdCl::XRootDStatus st = ex.Status();
    if( ! ( st.IsOK() || ( st.code == XrdCl::errDataError && st.errNo == XrdEc::IOError::ioTooManyErrors ) ) )
      return false;
    else
    {
      std::cout << "Too many errors while reading chunks!" << std::endl;
      return true;
    }
  }

  if( bytesrd != size )
  {
    std::cout << "bytes read: " << bytesrd << std::endl;
    std::cout << "read size: " << size << std::endl;
    std::cout << "Wrong size of read data!" << std::endl;
    return false;
  }

  auto b = input.begin() + offset;
  auto e = b + size;

  if( !std::equal( b, e, buffer.get() ) )
  {
    std::cout << "offset  : " << offset << std::endl;
    std::cout << "size    : " << size << std::endl;
    std::cout << "bytesrd : " << bytesrd << std::endl;
    std::cout << "input size : " << input.size() << std::endl;

//    PrintChunks( offset, size );

    std::cout << std::string( b, e ) << std::endl;
    std::cout << std::string( b, e ).size() << std::endl;
    std::cout << std::string( buffer.get(), bytesrd ) << std::endl;
    std::cout << std::string( buffer.get(), bytesrd ).size() << std::endl;

    std::cout << "Read data don't match the original input!" << std::endl;
    return false;
  }

  return true;
}

int ParallelRandomRead( XrdEc::DataStore &store, std::default_random_engine &generator )
{
  std::cout << __func__ << " : ";

  std::uniform_int_distribution<int> nbdistr( 2, 8 );
  uint8_t nbThreads = nbdistr( generator );
  std::vector<std::future<bool>> ftrs;

  for( uint8_t i = 0; i < nbThreads; ++i )
  {
    std::uniform_int_distribution<int> offdistr( 0, input.size() - 1 );
    uint64_t offset = offdistr( generator );
    std::uniform_int_distribution<int> sizedistr( 1, input.size() - offset );
    uint64_t size = sizedistr( generator );
    ftrs.emplace_back( std::async( &run_parallel_read, std::ref( store ), offset, size ) );
  }

  bool OK = true;
  for( uint8_t i = 0; i < nbThreads; ++i )
    OK = ( OK && ftrs[i].get() );

  if( !OK ) return 1;

  std::cout << "Well done!" << std::endl;

  return 0;
}

int ReadChunk()
{
  Cleanup();

  XrdEc::DataStore store( "dupa/jas" );

  store.Write( 0, input.size(), input.c_str() );
  store.Sync();

  std::default_random_engine generator( time( 0 ) );
  return ReadRandomChunk( store, generator );
}

int AppendRandomChunk( XrdEc::DataStore &store, std::default_random_engine &generator )
{
  std::cout << __func__ << " : ";

  std::uniform_int_distribution<int> offdistr( 0, input.size() - 1 );
  uint64_t choff = offdistr( generator );
  std::uniform_int_distribution<int> sizedistr( 1, input.size() - choff );
  uint64_t chsize = sizedistr( generator );

  std::string randch = input.substr( choff, chsize );

  store.Write( input.size(), randch.size(), randch.c_str() );
  store.Sync();

  std::uniform_int_distribution<int> offdiff( input.size() * 0.1, input.size() * 0.2 );
  uint64_t oldsize = offdiff( generator );
  uint64_t offset = input.size() - oldsize;
  uint64_t size   = randch.size() + oldsize;
  std::unique_ptr<char[]> buffer( new char[size] );

  uint64_t bytesrd = store.Read( offset, size, buffer.get() );

  if( bytesrd != size )
  {
    std::cout << "bytes read: " << bytesrd << std::endl;
    std::cout << "read size: " << size << std::endl;
    std::cout << "Wrong size of read data!" << std::endl;
    return 1;
  }

  input += randch;

  auto b = input.begin() + offset;
  auto e = b + size;

  if( !std::equal( b, e, buffer.get() ) )
  {
    std::cout << "Read data don't match the original input!" << std::endl;
    return 1;
  }

  std::cout << "Well done!" << std::endl;

  return 0;
}

int AppendChunk()
{
  Cleanup();

  XrdEc::DataStore store( "dupa/jas" );

  store.Write( 0, input.size(), input.c_str() );
  store.Sync();

  std::default_random_engine generator( time( 0 ) );
  return AppendRandomChunk( store, generator );
}

int OverwriteRandomChunk( XrdEc::DataStore &store, std::default_random_engine &generator )
{
  std::cout << __func__ << " : ";



  std::uniform_int_distribution<int> choffdistr( 0, input.size() * 0.8 );
  uint64_t choff = choffdistr( generator );
  std::uniform_int_distribution<int> chsizedistr( 0.1 * input.size(), 0.2 * input.size() );
  uint64_t chsize = chsizedistr( generator );
  std::string randch = input.substr( choff, chsize );
  std::uniform_int_distribution<int> offdistr( 256, 512 );
  uint64_t offset = offdistr( generator );

  store.Write( offset, randch.size(), randch.c_str() );
  store.Sync();

  if( offset + randch.size() > input.size() )
    input.resize( offset + randch.size() );
  auto dst = input.begin() + offset;
  std::copy( randch.begin(), randch.end(), dst );

  uint64_t rdoff  = offset > 512 ? offset - 512 : 0;
  uint64_t rdsize = 1024 + randch.size();
  std::unique_ptr<char[]> buffer( new char[rdsize] );
  std::fill( buffer.get(), buffer.get() + rdsize, '\0' );

  uint64_t bytesrd = store.Read( rdoff, rdsize, buffer.get() );

  auto b = input.begin() + rdoff;
  auto e = b + bytesrd;

  if( !std::equal( b, e, buffer.get() ) )
  {
    std::cout << "Read data don't match the original input!" << std::endl;
    return 1;
  }

  std::cout << "Well done!" << std::endl;

  return 0;
}

int OverwriteChunk()
{
  Cleanup();

  XrdEc::DataStore store( "dupa/jas" );

  store.Write( 0, input.size(), input.c_str() );
  store.Sync();

  std::default_random_engine generator( time( 0 ) );
  return OverwriteRandomChunk( store, generator );
}

int RandomShrink( XrdEc::DataStore &store, std::default_random_engine &generator )
{
  std::cout << __func__ << " : ";

  std::uniform_int_distribution<uint64_t> sizedistr( input.size() / 2, ( input.size() * 3 ) / 4 );
  uint64_t size = sizedistr( generator );

  input.resize( size );
  store.Truncate( size );

  std::uniform_int_distribution<uint64_t> rddistr( input.size() * 0.1, input.size() * 0.2 );
  uint64_t tmp = rddistr( generator );

  uint64_t rdoff  = input.size() - tmp;
  uint64_t rdsize = 2 * tmp;
  std::unique_ptr<char[]> buffer( new char[rdsize] );
  uint64_t bytesrd = store.Read( rdoff, rdsize, buffer.get() );

  if( bytesrd != tmp )
  {
    std::cout << "bytes read after truncating : " << bytesrd << std::endl;
    std::cout << "expected : " << tmp << std::endl;
    std::cout << "read offset : " << rdoff << std::endl;
    std::cout << "file size : " << size << std::endl;

    std::cout << std::endl;
    std::cout << input.substr( rdoff, bytesrd ) << std::endl;
    std::cout << std::endl;
    std::cout << std::string( buffer.get(), bytesrd ) << std::endl << std::endl;

    return 1;
  }

  auto b = input.begin() + rdoff;
  auto e = b + tmp;

  if( !std::equal( b, e, buffer.get() ) )
  {
    std::cout << std::endl;
    std::cout << input.substr( rdoff, bytesrd ) << std::endl;
    std::cout << std::endl;
    std::cout << std::string( buffer.get(), bytesrd ) << std::endl << std::endl;
    std::cout << "Read data don't match the original input!" << std::endl;
    return 1;
  }

  std::cout << "Well done!" << std::endl;

  return 0;
}

int RandomSparseWrite( XrdEc::DataStore &store, std::default_random_engine &generator )
{
  std::cout << __func__ << " : ";

  XrdEc::Config &cfg = XrdEc::Config::Instance();

  std::uniform_int_distribution<int> offdistr( input.size() + cfg.datasize / 2 , input.size() + cfg.datasize * 3 );
  uint64_t wrtoff = offdistr( generator );
  std::uniform_int_distribution<int> sizedistr( 0.1 * input.size(), 0.2 * input.size()  );
  uint64_t wrtsize = sizedistr( generator );
  std::uniform_int_distribution<int> choffdistr( 0, input.size() - 1 - wrtsize );
  uint64_t choff = choffdistr( generator );


  std::string randch = input.substr( choff, wrtsize );

  store.Write( wrtoff, wrtsize, randch.c_str() );
  store.Sync();

  std::uniform_int_distribution<int> offdiff( input.size() * 0.1, input.size() * 0.2 );
  uint64_t oldsize = offdiff( generator );
  uint64_t offset = input.size() - oldsize;
  uint64_t size   = wrtoff - offset + randch.size() - 0.7 * randch.size();
  std::unique_ptr<char[]> buffer( new char[size] );
  std::fill( buffer.get(), buffer.get() + size, 'A' );

  uint64_t bytesrd = store.Read( offset, size, buffer.get() );

  if( bytesrd != size )
  {
    std::cout << "bytes read: " << bytesrd << std::endl;
    std::cout << "read size: " << size << std::endl;
    std::cout << "Wrong size of read data!" << std::endl;
    return 1;
  }

  input.resize( wrtoff, 0 );
  input += randch;

  auto b = input.begin() + offset;
  auto e = b + size;

  if( !std::equal( b, e, buffer.get() ) )
  {
    std::cout << "Read data don't match the original input!" << std::endl;
    std::cout << "Size : " << e - b << std::endl;
    return 1;
  }

  std::cout << "Well done!" << std::endl;

  return 0;
}

int RandomGrow( XrdEc::DataStore &store, std::default_random_engine &generator )
{
  std::cout << __func__ << " : ";

  std::uniform_int_distribution<uint64_t> sizedistr( 1.1 * input.size(), 1.3 * input.size() );
  uint64_t size = sizedistr( generator );

  size_t orgsize = input.size();

  input.resize( size, 0 );
  store.Truncate( size );

  std::uniform_int_distribution<uint64_t> rddistr( orgsize * 0.1, orgsize * 0.2 );
  uint64_t tmp = rddistr( generator );

  uint64_t rdoff  = input.size() - tmp;
  uint64_t rdsize = 2 * tmp;
  std::unique_ptr<char[]> buffer( new char[rdsize] );
  uint64_t bytesrd = store.Read( rdoff, rdsize, buffer.get() );

  if( bytesrd != tmp )
  {
    std::cout << "bytes read after truncating : " << bytesrd << std::endl;
    std::cout << "expected : " << tmp << std::endl;
//
//    std::cout << std::endl;
//    std::cout << input.substr( rdoff, bytesrd ) << std::endl;
//    std::cout << std::endl;
//    std::cout << std::string( buffer.get(), bytesrd ) << std::endl << std::endl;

    return 1;
  }

  auto b = input.begin() + rdoff;
  auto e = b + tmp;

  if( !std::equal( b, e, buffer.get() ) )
  {
    std::cout << std::endl;
    std::cout << input.substr( rdoff, bytesrd ) << std::endl;
    std::cout << std::endl;
    std::cout << std::string( buffer.get(), bytesrd ) << std::endl << std::endl;
    std::cout << "Read data don't match the original input!" << std::endl;
    return 1;
  }

  std::cout << "Well done!" << std::endl;

  return 0;
}

int RandomizedTests( time_t seed )
{
  ++runnb;

  std::cout << "Original size: " << input.size() << std::endl;

  Cleanup();

  QDBSetup();

  XrdEc::DataStore store( "dupa/jas" );

  store.Write( 0, input.size(), input.c_str() );
  store.Sync();
  std::cout << __func__ << ": seed = " << seed << std::endl;
  std::default_random_engine generator( seed );

  std::uniform_int_distribution<int> funcid( 0, 6 );
  typedef int (*randfunc)( XrdEc::DataStore&, std::default_random_engine& );
  randfunc functions[] = { ReadRandomChunk, AppendRandomChunk, OverwriteRandomChunk, RandomShrink, RandomSparseWrite, RandomGrow, ParallelRandomRead };

  for( int i = 0; i < 60; ++i )
  {
    std::cout << i << ": ";

    int ret = functions[funcid( generator )]( store, generator );
    if( ret )
    {
      std::cout << ">>>>>>>>>>>>>>>>>>>> Test failed!" << std::endl;
      return ret;
    }
  }

  store.Finalize();

  std::cout << "Final size: " << input.size() << std::endl;

  return 0;
}

int main( int argc, char** argv )
{
  std::cout << "There we go!" << std::endl;

  int rc = 0;
  try
  {
    rc = RandomizedTests ( time( 0 ) );
    if( rc ) return rc;
    std::cout << std::endl << "And now the second round so we know the version and placement were correctly preserved!" << std::endl << std::endl;
    rc = RandomizedTests( time( 0 ) );
  }
  catch( std::exception &ex )
  {
    std::cout << ">>>>>>>>>>>>>>>>>>>>" << ex.what() << std::endl;
    return 1;
  }
  if( rc ) return rc;

//  Cleanup();

  std::cout << "The End." << std::endl;

  return 0;
}


