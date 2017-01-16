library(ggplot2)
library(dplyr)
str(mpg)

mpg %>% ggplot(aes(x=cty, y=hwy)) +
  geom_point(aes(color = factor(year),
                 size = displ),
                 alpha = 0.5,
                 position = 'jitter') +

  stat_smooth() +

  scale_color_manual(values = c('steelblue','red4')) +
  scale_size_continuous(range = c(4,10)) +

  coord_cartesian() +

  facet_wrap(~ year, ncol = 1)




