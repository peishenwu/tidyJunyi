
AirPassengers  %>%  as.data.frame %>%
  mutate(row = sample(as.Date("2016-1-1")+1:300,144,replace = T))  %>%
  group_by(row)  %>% summarize(count = sum(x),
                               count2 = 2*mean(x))  %>% plot_timeseries(row,count,count2)
