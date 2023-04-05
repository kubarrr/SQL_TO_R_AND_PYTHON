#Jakub Rymarski

#1 Rozwi¹zania korzystaj¹ce z sqldf::sqldf();

df_sql_1 <- function(Tags){
  return(sqldf("SELECT Count, TagName
               FROM Tags
               WHERE Count > 1000
               ORDER BY Count DESC"))
}

df_sql_2 <- function(Users, Posts){
  return(sqldf("SELECT Location, COUNT(*) AS Count
               FROM (
                 SELECT Posts.OwnerUserId, Users.Id, Users.Location
                 FROM Users
                 JOIN Posts ON Users.Id = Posts.OwnerUserId
               )
               WHERE Location NOT IN ('')
               GROUP BY Location
               ORDER BY Count DESC
               LIMIT 10"))
}

df_sql_3 <- function(Badges){
  return(sqldf("SELECT Year, SUM(Number) AS TotalNumber
               FROM (
                 SELECT
                 Name,
                 COUNT(*) AS Number,
                 STRFTIME('%Y', Badges.Date) AS Year
                 FROM Badges
                 WHERE Class = 1
                 GROUP BY Name, Year
               )
               GROUP BY Year
               ORDER BY TotalNumber"))
}

df_sql_4 <- function(Users, Posts){
  return(sqldf("SELECT
               Users.AccountId,
               Users.DisplayName,
               Users.Location,
               AVG(PostAuth.AnswersCount) as AverageAnswersCount
               FROM
               (
                 SELECT
                 AnsCount.AnswersCount,
                 Posts.Id,
                 Posts.OwnerUserId
                 FROM (
                   SELECT Posts.ParentId, COUNT(*) AS AnswersCount
                   FROM Posts
                   WHERE Posts.PostTypeId = 2
                   GROUP BY Posts.ParentId
                 ) AS AnsCount
                 JOIN Posts ON Posts.Id = AnsCount.ParentId
               ) AS PostAuth
               JOIN Users ON Users.AccountId=PostAuth.OwnerUserId
               GROUP BY OwnerUserId
               ORDER BY AverageAnswersCount DESC, AccountId ASC
               LIMIT 10"))
}

df_sql_5 <- function(Posts, Votes){
  return(sqldf("SELECT Posts.Title, Posts.Id,
               STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date,
               VotesByAge.Votes
               FROM Posts
               JOIN (
                 SELECT
                 PostId,
                 MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes,
                 MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes,
                 SUM(Total) AS Votes
                 FROM (
                   SELECT
                   PostId,
                   CASE STRFTIME('%Y', CreationDate)
                   WHEN '2021' THEN 'new'
                   WHEN '2020' THEN 'new'
                   ELSE 'old'
                   END VoteDate,
                   COUNT(*) AS Total
                   FROM Votes
                   WHERE VoteTypeId IN (1, 2, 5)
                   GROUP BY PostId, VoteDate
                 ) AS VotesDates
                 GROUP BY VotesDates.PostId
                 HAVING NewVotes > OldVotes
               ) AS VotesByAge ON Posts.Id = VotesByAge.PostId
               WHERE Title NOT IN ('')
               ORDER BY Votes DESC
               LIMIT 10"))
}


#2 Rozwi¹zania korzystaj¹ce z funkcji bazowych

df_base_1 <- function(Tags){
  # wybieramy kolumny Count i TagName, gdzie Count>1000
  df <- Tags[Tags$Count>1000, c("Count", "TagName")]
  
  # sortujemy malej¹co po Count
  df <- df[order(df$Count, decreasing = TRUE), ]
  
  
  # przywracamy domyœlne numerowanie wierszy
  rownames(df) <- NULL
  
  return(df)
  
}

df_base_2<-function(Users, Posts){
  
  #tworzymy tabele pomocnicze
  df_users<-Users[c("Id", "Location")]
  df_posts<-Posts["OwnerUserId"]
  
  #£¹czymy je, wybiermay tylko interesuj¹ce nas wiersze gdzie Location!=''
  #z pomoc¹ drop=FALSE otrzymujemy ramkê danych
  df<-merge(df_users, df_posts, by.x="Id", by.y="OwnerUserId")
  df<-df[df$Location!='', "Location", drop=FALSE]
  
  #zliczamy wyst¹piena dla danej Location
  df<-aggregate(x=df["Location"],
                       by=df["Location"],
                       FUN=length)
  
  #zmieniamy nazwy kolumn, sortujemy i zwracamy pierwsze 10 wierszy
  colnames(df)<-c("Location", "Count")
  df<-head(df[order(df$Count, decreasing = TRUE),], 10)
  
  # przywracamy domyœlne numerowanie wierszy
  rownames(df) <- NULL
  
  return(df)
}

df_base_3<-function(Badges){
  
  #tworzymy tabelê pomocnicz¹ do wydobycia roku z ca³ej daty
  df_year<-Badges$Date
  df_year<-substring(df_year, 1, 4)
  
  #dodajemy wektor z rokiem do kolumn Class i Name z tabeli Badges
  #zmieniamy nazwy kolumn
  #Wybiermay kolumny Name i Year gdzie Class==1
  df<-cbind(Badges[c("Class","Name")],df_year)
  colnames(df)<-c("Class", "Name", "Year")
  df<-df[df$Class==1, c("Name", "Year")]
  
  # zliczamy wyst¹pienia danego imienia grupuj¹æ po Name i Year
  #zmieniamy nazwy i kolejnoœæ kolumn
  df<-aggregate(x=df["Name"],
                       by=df[c("Name", "Year")],
                       FUN=length)
  colnames(df)[3]<-"Number"
  df<-df[c("Name", "Number", "Year")]
  
  # sumujemy Number dla danego Year
  #zmieniamy nazwê kolumny
  #sortujemy
  df<-aggregate(x=df["Number"],
                 by=df["Year"],
                 FUN=sum)
  colnames(df)[2]<-"TotalNumber"
  df<-df[order(df$TotalNumber, decreasing = FALSE),]
  
  # przywracamy domyœlne numerowanie wierszy
  rownames(df) <- NULL
  
  return(df)
  
}


df_base_4<-function(Users, Posts){
  
  #tworzymy tabelê pomocnicz¹ AnsCount
  #wybieramy te pola ParentId z ramki Posts, gdzie PostTypeId==2
  #z pomoc¹ drop = FALSE otrzymujemy ramkê danych
  AnsCount<-Posts[Posts$PostTypeId==2, "ParentId", drop = FALSE]
  
  #zliczamy liczbê wyst¹pieñ dla danego ParentId i zmieniamy nazwê kolumny
  AnsCount<-aggregate(x=AnsCount$ParentId,
                      by=AnsCount["ParentId"],
                      FUN=length)
  colnames(AnsCount)[2]<-"AnswersCount"
  
  #³¹czymy ramkê Posts z AnsCount, wybieraj¹c odpowiednie kolumny
  PostAuth<-merge(AnsCount,
                  Posts[c("Id","OwnerUserId")],
                  by.x="ParentId", by.y="Id")
  
  #zmieniamy nazwê kolumny ParentId na Id i zmieniamy kolejnoœæ kolumn
  colnames(PostAuth)[1]<-"Id"
  PostAuth<-PostAuth[c("AnswersCount", "Id", "OwnerUserId")]
  
  #zliczamy œrednie z AnswersCount dla danego OwnerUserId i zmieniamy nazwê kolumny
  PostAuth<-aggregate(x=PostAuth$AnswersCount,
                by=PostAuth["OwnerUserId"],
                FUN=mean)
  colnames(PostAuth)[2]<-"AverageAnswersCount"
  
  #³aczymy Users z PostAuth, wybieraj¹c odpowiednie kolumny
  #zmieniamy nazwê kolumny i zmieniamy kolejnoœæ kolumn
  df<-merge(PostAuth,
            Users[c("AccountId", "DisplayName", "Location")],
            by.x="OwnerUserId", by.y="AccountId")
  colnames(df)[1]<-"AccountId"
  df<-df[c("AccountId", "DisplayName", "Location", "AverageAnswersCount")]
  
  #sortujemy po AverageAnswersCount DESC i AccountId ASC, zwracamy pierwsze 10 wierszy
  df<-head(df[order(df$AverageAnswersCount, df$AccountId, decreasing=c(T, F)),], 10)
  
  # przywracamy domyœlne numerowanie wierszy
  rownames(df) <- NULL
  
  return(df)
}


df_base_5<-function(Posts, Votes){
  
  #wydobybawy rok z daty utworzenia
  df_year<-Votes$CreationDate
  df_year<-substring(df_year, 1, 4)
  
  #zmieniamy lata 2021 i 2020 na "new", a resztê na "old"
  VoteDate <- sapply(df_year, switch,
                              "2021" = "new", 
                              "2020" = "new",
                              "old")
  VoteDate<-as.data.frame(VoteDate)
  
  #³aczymy dane kolumny z Votes i VOteDate, a nastêpnie wybieramy odpowiednie pola
  VotesDates<-cbind(Votes[c("PostId", "VoteTypeId")], VoteDate)
  VotesDates<-VotesDates[VotesDates$VoteTypeId %in% c("1", "2", "5") , c("PostId", "VoteDate")]
  
  #zliczamy liczbê wyst¹pieñ dla danego PostId i VoteDate, zmieniamy nazwê kolumny
  VotesDates<-aggregate(x=VotesDates$VoteDate,
                        by=VotesDates[c("PostId", "VoteDate")],
                        FUN=length)
  colnames(VotesDates)[3]="Total"
  
  # tworzymy kolumny NewVotes, OldVotes, a nastêpnie VotesDates i zmieniamy kolejnoœæ kolumn
  NewVotes<-transform(VotesDates, VoteDate = ifelse(VotesDates$VoteDate == "new", VotesDates$Total, 0))
  OldVotes<-transform(VotesDates, VoteDate = ifelse(VotesDates$VoteDate == "old", VotesDates$Total, 0))
  VotesDates<-cbind(NewVotes[, 1:2], OldVotes[, 2:3])
  colnames(VotesDates)<- c("PostId", "NewVotes", "OldVotes", "Total")
  
  #znajdujemy dane wartoœci maksymalne i sumy, a nastêpnie ³¹cz¹c kolumny tworzymy VotesDates
  pom1<-aggregate(x=VotesDates$NewVotes,
                  by = VotesDates[c("PostId")],
                  FUN=max)
  pom2<-aggregate(x=VotesDates$OldVotes,
                  by = VotesDates[c("PostId")],
                  FUN=max)
  pom3<-aggregate(x=VotesDates["Total"],
                  by = VotesDates[c("PostId")],
                  FUN=sum)
  VotesDates<-cbind(pom1[1], pom1[2], pom2[2], pom3[2])
  colnames(VotesDates)<- c("PostId", "NewVotes", "OldVotes", "Votes")
  
  #wybieramy tylko te pola, gdzie NewVotes>OldVotes
  VotesByAge<-VotesDates[VotesDates$NewVotes > VotesDates$OldVotes, ]
  
  #wydobywamy datê w formacie Y-m-d z daty utworzenia postu i ³¹czymy odpowiednie kolumny
  df_date<-Posts$CreationDate
  df_date<-substring(df_date, 1, 10)
  df<-cbind(Posts[c("Title", "Id")], df_date)
  colnames(df)[3]<-"Date"
  
  #£¹czymy odpowiednie kolumny z Posts z Votes
  df<-merge(x=df,
            y=VotesByAge[c("PostId", "Votes")],
            by.x="Id", by.y="PostId")
  df<-df[c("Title", "Id", "Date", "Votes")]
  df<-as.data.frame(df)
  
  #wybieramy pola, gdzie Title!=''
  #sortujemy wzglêdem Votes malej¹co i zwracamy pierwsze 10 wierszy
  # przywracamy domyœlne numerowanie wierszy
  df<-df[df$Title!='',]
  df<-head(df[order(df$Votes, decreasing=TRUE),], 10)
  rownames(df)<-NULL
  return(df)
}



#Rozwi¹zania korzystaj¹ce z dplyr

df_dplyr_1 <- function(Tags){
  
  #za pomoc¹ select wybieramy kolumny Count i TagName z tabeli Tags
  df<-select(Tags, Count, TagName) %>%
    # filtrujemy, czyli wybieramy tylko te wiersze gdzie Count>1000
    filter(Count > 1000) %>%
    #sortujemy malej¹co po Count
    arrange(desc(Count))
  
  return (df)
}


df_dplyr_2 <- function(Users,Posts){
  
  #za pomoc¹ select wybieramy kolumny Id i Location z tabeli Users
  df <- select(Users, Id, Location) %>%
    #wybieramy tylko te wiersze gdzie Location!=""
    filter(Location!="") %>%
    #³¹czymy aktualn¹ tabelê z odpowiednimi kolumnami z Posts
    inner_join(x=.,
               y = select(Posts,OwnerUserId),
               by = c("Id"="OwnerUserId")) %>%
    #grupujemy po Location
    group_by(Location) %>% 
    #zliczamy liczbê wyst¹pieñ dla danej Location
    summarise(Location,
              Count=length(Location),
              .groups = "keep") %>%
    #pozbywamy siê duplikatów wierszy
    distinct() %>%
    #sortujemy malej¹co po Count
    arrange(desc(Count)) %>%
    #wybieramy 10 pierwszych wierszy
    head(10) %>%
    #zamieniamy na ramkê danych
    as.data.frame()
    
    return(df)
}


df_dplyr_3<- function(Badges){
  #tworzymy tabelê pomocnicz¹ do wydobycia roku z ca³ej daty
  df_year<-Badges$Date
  df_year<-substr(df_year, 1, 4)
  
  #tworzymy tabelê, ³¹cz¹c odpowiednie kolumny z Badges i df_year
  df<-bind_cols(select(Badges,Class,Name), "Year"=df_year) %>% 
    #filtrujemy, wybieramy tylko wiersze gdzie Class==1
    filter(Class==1) %>% 
    #pozbywamy siê niepotrzebnej kolumny Class
    select(-Class) %>%
    #grupujemy wed³ug Name i Year
    group_by(Name, Year) %>% 
    #zliczamy liczbê wyst¹pieñ Name dla danych Name i Year
    summarise(Name,
              Number=length(Name),
              Year,
              .groups = "keep") %>%
    #grupujemy wed³ug Year
    group_by(Year) %>%
    #pozbywamy siê duplikatów wierszy
    distinct() %>%
    #sumujemy Number dla danego roku
    summarise(Year,
              TotalNumber=sum(Number),
              .groups = "keep") %>%
    #sortujemy po TotalNumber
    arrange(TotalNumber) %>%
    #pozbywamy siê duplikatów wierszy
    distinct() %>%
    #zamieniamy na ramkê danych
    as.data.frame()
  return(df)
  
}


df_dplyr_4<- function(Users, Posts){
  
  #tworzymy tabelê AnSCount, wybieraj¹c odpowiednie kolumny z Posts
  AnsCount<-select(Posts, ParentId, PostTypeId) %>%
    #filtrujemy, wybiermay tylko wiersze gdzie PostTypeId==2
    filter(PostTypeId==2) %>%
    #pozbywamy siê kolumny PostTypeId
    select(-PostTypeId) %>%
    #grupujemy wed³ug ParentId
    group_by(ParentId) %>%
    #zliczamy liczbê wyst¹pieñ danych ParentId i pozbywamy siê duplikatów wierszy
    summarise(ParentId,
              AnswersCount=length(ParentId),
              .groups="keep") %>%
    distinct() 
  
  #Tworzymy tabelê PostAuth, ³¹cz¹c AnsCount z odpowiednimi kolumnami z Posts
  PostAuth<-inner_join(x=AnsCount,
         y=select(Posts, Id, OwnerUserId),
         by=c("ParentId"="Id")) %>%
    #zmieniamy nazwê i kolejnoœæ kolumn i grupujemy wed³ug OwnerUserId
    rename(.,"Id"=ParentId) %>%
    relocate(Id,.after=AnswersCount) %>% 
    group_by(OwnerUserId)
    
  #³aczymy PostAuth z odpowienimi kolumnami z Users i zmieniamy nazwê i kolejnoœæ kolumn
  df<-inner_join(x=PostAuth,
                 y=select(Users, AccountId, DisplayName, Location),
                 by=c("OwnerUserId"="AccountId")) %>%
    rename(.,"AccountId"=OwnerUserId) %>%
    relocate(AnswersCount,.after=Location) %>%
    #liczymy œredni¹ AnswerCount dla danego OwnerUsrerId i pozbywamy siê duplikatóW wierszy
    summarise(AccountId, DisplayName, Location,
              AverageAnswersCount=mean(AnswersCount),
              .groups="keep") %>%
    distinct() %>%
    #sortujemy malej¹co po AverageAnswersCount i rosn¹co po AccountId
    arrange(desc(AverageAnswersCount), AccountId) %>%
    #wybiermay pierwsze 10 wierszy
    head(10) %>%
    #zamieniamy na ramkê danych
    as.data.frame()
  
  return(df)
}


df_dplyr_5<-function(Posts, Votes){
  df_year<-Votes$CreationDate
  df_year<-substr(df_year, 1, 4)
  VotesByAge<-bind_cols(select(Votes,PostId, VoteTypeId), "VoteDate"=df_year) %>%
    filter(VoteTypeId %in% c(1,2,5)) %>% 
    group_by(PostId, VoteDate = case_when(VoteDate == "2021" | VoteDate == "2020" ~ "new", TRUE ~ "old")) %>%
    mutate(Total = n()) %>%
    select(PostId, VoteDate, Total) %>% 
    group_by(PostId) %>% 
    distinct() %>%
    mutate(NewVotes = max(case_when(VoteDate == "new" ~ as.integer(Total), TRUE ~ as.integer(0)))) %>% 
    mutate(OldVotes = max(case_when(VoteDate == "old" ~ as.integer(Total), TRUE ~ as.integer(0)))) %>% 
    filter(NewVotes>OldVotes) %>%
    group_by(PostId, NewVotes, OldVotes) %>%
    summarise(Votes = sum(Total), .groups = 'keep')
  
  df<-inner_join(x=select(Posts, Title, Id, CreationDate),
                 y=VotesByAge,
                 by=c("Id"="PostId")) %>%
    select(Title, Id, CreationDate, Votes) %>%
    filter(Title!="")
  
  Date<-substr(df$CreationDate, 1, 10)
  
  df<-select(df, -CreationDate)
  df<-bind_cols(df, "Date"=Date) %>%
    relocate(Votes,.after=Date) %>%
    arrange(desc(Votes)) %>%
    head(10) %>%
    as.data.frame()
  
  return(df)
}



#Rozwi¹zania korzystaj¹ce z data.table

df_table_1 <- function(Tags){
  
  # zmieniamy ramkê danych na tabelê
  TagsT <- as.data.table(Tags)
  
  #wybieramy kolumny Count i TagName, z tabeli Tags, gdzie Count>1000
  df<-TagsT[Count>1000, .(Count, TagName)][
    #sortujemy malej¹co po Count 
    order(-Count)]
  # zmieniamy na ramkê danych
  df<-as.data.frame(df)
  return(df)
  
  
}


df_table_2 <- function(Users,Posts){
  
  # zmieniamy ramki danych na tabele
  UsersT <- as.data.table(Users)
  PostsT <- as.data.table(Posts)
  
  #³¹czymy tabele Users z Post
  df <- UsersT[, .(Id, Location)
    ][ PostsT, on = .(Id = OwnerUserId), nomatch = 0
    #wybieramy odpowienie kolumny i wybiermay wiersze, gdzie Location!=""
    ][, .( Id, Location)
      ][Location!="",
        #zliczamy iloœæ wyst¹pieñ dla danej Location
        ][, .(Count=.N), by=.(Location)
          #sortujemy malej¹co po Count i zwracamy pierwsze 10 wierszy
          ][order(-Count)][1:10]
  
  # zmieniamy na ramkê danych
  df<-as.data.frame(df)
  return(df)
}


df_table_3 <- function(Badges){
  
  BadgesT <-as.data.table(Badges)
  
  df_year<-Badges$Date
  df_year<-substring(df_year, 1, 4)
  df<-cbind(BadgesT[, .(Class, Name)], Year=df_year)
  df<-df[Class==1
         ][, .(Name, Year)
           ][, .(Number=.N), by=.(Name, Year)
             ][, .(Name, Number, Year)
               ][, .(TotalNumber = sum(Number)), by=.(Year)
                 ][order(TotalNumber)]
  
  df<-as.data.frame(df)
  return(df)
}


df_table_4 <- function(Users, Posts){
  
  UsersT <-as.data.table(Users)
  PostsT <-as.data.table(Posts)
  
  AnsCount<-PostsT[PostTypeId==2
                  ][, .(ParentId)
                    ][, .(AnswersCount=.N), by=.(ParentId)]
  
  PostAuth<-AnsCount[ PostsT, on = .(ParentId = Id), nomatch = 0
                      ][, .(AnswersCount, Id=ParentId, OwnerUserId)
                        ][, .(AverageAnswersCount=mean(AnswersCount)), by=.(OwnerUserId)]
  
  df<-PostAuth[UsersT[, .(AccountId, DisplayName, Location)], on= .(OwnerUserId=AccountId), nomatch = 0
               ][, .(AccountId=OwnerUserId, DisplayName, Location, AverageAnswersCount)
                 ][order(-AverageAnswersCount, AccountId)][1:10]
  
  df<-as.data.frame(df)
  return(df)
}


df_table_5<-function(Posts, Votes){
  PostsT <-as.data.table(Posts)
  VotesT <-as.data.table(Votes)
  
  df_year<-Votes$CreationDate
  df_year<-substring(df_year, 1, 4)
  pom<-cbind(VotesT[, .(PostId, VoteTypeId)], Year=df_year)
  pom<-pom[VoteTypeId %in% c(1, 2, 5),
           ][, VoteDate := ifelse(Year %in% c("2021", "2020"), "new", "old")
             ][, .(PostId, VoteDate)
               ][, .(Total =.N), by=.(PostId, VoteDate)]
  VotesDates<-pom[, ':='(NewVotes = ifelse(VoteDate=="new", Total, 0), OldVotes = ifelse(VoteDate=="old", Total, 0))
                  ][, .(PostId, NewVotes, OldVotes, Total)]
  VotesByAge<-VotesDates[, .(NewVotes = max(NewVotes), OldVotes=max(OldVotes), Votes = sum(Total)), by=.(PostId)
                         ][NewVotes>OldVotes]
  
  df_date<-Posts$CreationDate
  df_date<-substring(df_date, 1, 10)
  pom2<-cbind(PostsT[, .(Title, Id)], Date=df_date)
  df<-VotesByAge[pom2, on=.(PostId=Id), nomatch=0
                 ][, .(Title, Id=PostId, Date, Votes)
                   ][Title !=""
                     ][order(-Votes)][1:10]
  df<-as.data.frame(df)
  return(df)
}


