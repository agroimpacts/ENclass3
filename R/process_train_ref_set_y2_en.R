# Process model results from AWS instance used to process data
# for season 2

library(dplyr)
library(sf)
library(tidyr)
library(glue)
# library(parallel)
# get data from instance
dataset <- "/home/rstudio/projects/data/extract/"

dat2 <- lapply(dir(dataset, full.names = TRUE), function(x) {
  readr::read_csv(x) %>%
    mutate(tile = gsub("tile_|_dataset.csv", "", basename(x))) %>%
    select(ID, id, fid, tile, !!names(.))
})
dat2 <- do.call(rbind, dat2) %>% select(-crop, -X1) %>%
  select(ID, id, fid, tile, class, weight, !!names(.))


#dat2 %>% View()
# add Sentinel-2 indices
dat2 <- dat2 %>%
  mutate(
    ndvi_1 = (B8_1 - B4_1) / (B8_1 + B4_1),
    ndvi_2 = (B8_2 - B4_2) / (B8_2 + B4_2),
    rg1_ndvi_1 = (B8_1 - B5_1) / (B8_1 + B5_1),
    rg1_ndvi_2 = (B8_2 - B5_2) / (B8_2 + B5_2),
    rg2_ndvi_1 = (B8_1 - B6_1) / (B8_1 + B6_1),
    rg2_ndvi_2 = (B8_2 - B6_2) / (B8_2 + B6_2),

    gcvi_1 = (B8_1 / (B3_1 + 0.00001)) - 1,
    gcvi_2 = (B8_2 / (B3_2 + 0.00001)) - 1,
    rg1_gcvi_1 = (B8_1 / (B5_1 + 0.00001)) - 1,
    rg1_gcvi_2 = (B8_2 / (B5_2 + 0.00001)) - 1,
    rg2_gcvi_1 = (B8_1 / (B6_1 + 0.00001)) - 1,
    rg2_gcvi_2 = (B8_2 / (B6_2 + 0.00001)) - 1,

    mtci_1 = (B8_1 - B5_1) / (B5_1 - B4_1 + 0.00001),
    mtci_2 = (B8_2 - B5_2) / (B5_2 - B4_2 + 0.00001),
    mtci2_1 = (B6_1 - B5_1) / (B5_1 - B4_1 + 0.00001),
    mtci2_2 = (B6_2 - B5_2) / (B5_2 - B4_2 + 0.00001),

    reip_1 = 700 + 40 * ((B4_1 + B7_1) / 2 - B5_1) / (B7_1 - B5_1),
    reip_2 = 700 + 40 * ((B4_2 + B7_2) / 2 - B5_2) / (B7_2 - B5_2),

    nbr1_1 = (B8_1 - B11_1) / (B8_1 + B11_1),
    nbr1_2 = (B8_2 - B11_2) / (B8_2 + B11_2),
    nbr2_1 = (B8_1 - B12_1) / (B8_1 + B12_1),
    nbr2_2 = (B8_2 - B12_2) / (B8_2 + B12_2),

    ndti_1 = (B11_1 - B12_1) / (B11_1 + B12_1),
    ndti_2 = (B11_2 - B12_2) / (B11_2 + B12_2),

    crc_1 = (B11_1 - B3_1) / (B11_1 + B3_1),
    crc_2 = (B11_2 - B3_2) / (B11_2 + B3_2),

    sti_1 = B11_1 / (B12_1 + 0.00001),
    sti_2 = B11_2 / (B12_2 + 0.00001)
)


na_ct <- function(x) length(which(is.na(x))) / n()
inf_ct <- function(x) length(which(is.infinite(x))) / n()


# Some of NAs but no infs
# dat %>% filter(is.na(weight))
dat2 %>% group_by(id) %>%
  summarize_at(vars(B2_1:sti_2), na_ct) %>%
  summarize_at(vars(B2_1:sti_2), max) %>% unlist()
dat2 %>% group_by(id) %>%
  summarize_at(vars(B2_1:sti_2), inf_ct) %>%
  summarize_at(vars(B2_1:sti_2), max) %>% unlist()
dat2 %>% group_by(id) %>%
  filter(is.infinite(B5_1))


# filer out ids = -Inf
dat2 <- dat2 %>% filter(id != -Inf)

# back-fill NA values with mean for field
datf <- dat2 %>% group_by(id) %>%
  mutate_at(vars(B2_1:sti_2), zoo::na.aggregate) %>%
  ungroup()
# fixed
datf %>% group_by(id) %>%
  summarize_at(vars(B2_1:sti_2), na_ct) %>%
  summarize_at(vars(B2_1:sti_2), max) %>% unlist()

# set.seed(10)
# samp <- sample.split(datf$id, SplitRatio = 0.8)
# train <- subset(dset, samp == TRUE)
# test <- subset(dset, samp == FALSE)
datf <- datf %>% drop_na(vh_1)
# should returns FALSE after fixing
any(is.na(rowSums(datf %>% select(B2_1:sti_2))))

#we have weight
#colnames(datf)

##########
# maize subsets
maize_wmeans <- lapply(seq(50, 150, by = 50), function(x) {  # x <- 1
  set.seed(x)
  datf %>% filter(class == "Maize") %>%
    group_by(id) %>%
    filter(weight > 0.5) %>%
    sample_n(size = ceiling(n() * 0.1), replace = TRUE) %>%
    summarize_at(vars(sti_2:weight), weighted.mean, w = quo(weight), mean, na.rm = TRUE) %>%
    mutate(seed = x) %>%
    select(seed, id, !!names(.))
}) %>% do.call(rbind, .) %>%
  mutate(class = "Maize")

#maize_wmeans %>% View()
#colnames(maize_wmeans)
#colnames(rice_wmeans)
#colnames(other_wmeans)
#colnames(noncrop_means)


#colnames(train_ref_mu)



#nrow(other_wmeans)
#nrow(noncrop_means)


##########
# rice subsets
rice_wmeans <- lapply(seq(30, 120, by = 30), function(x) {  # x <- 1
  set.seed(x)
  datf %>% filter(class == "Rice") %>%
    group_by(id) %>%
    filter(weight > 0.5) %>%
    sample_n(size = ceiling(n() * 0.1), replace = TRUE) %>%
    summarize_at(vars(sti_2:weight), weighted.mean, w = quo(weight), mean, na.rm = TRUE) %>%
    mutate(seed = x) %>%
    select(seed, id, !!names(.))
}) %>% do.call(rbind, .) %>%
  mutate(class = "Rice")
# count() %>% pull(n) %>% hist()

##########
# other subsets
other_wmeans <- lapply(seq(150, 300, by = 150), function(x) {  # x <- 1
  set.seed(x)
  datf %>% filter(class == "Other") %>%
    group_by(id) %>%
    sample_n(size = ceiling(n() * 0.05), replace = TRUE) %>%
    summarize_at(vars(sti_2:weight), weighted.mean, w = quo(weight), mean, na.rm = TRUE) %>%
    mutate(seed = x) %>%
    select(seed, id, !!names(.))
}) %>% do.call(rbind, .) %>%
  mutate(class = "Other")
# other_means %>% filter(id == 2)

# noncrop
set.seed(1)
datf %>% filter(class == "noncrop") %>%
  distinct(id) %>% sample_frac()

ssize <- mean(c(nrow(maize_wmeans), nrow(rice_wmeans), nrow(other_wmeans)))
set.seed(1)
noncrop_means <- datf %>% filter(class == "noncrop") %>%
  group_by(id) %>%
  summarize_at(vars(B2_1:sti_2), mean, na.rm = TRUE) %>%
  ungroup() %>% mutate(class = "noncrop", seed = NA) %>%
  sample_n(size = ssize)
any(is.na(rowSums(noncrop_means %>% select(B2_1:sti_2))))
# noncrop_med <- datf %>% filter(class == "noncrop") %>%
#   group_by(id) %>%
#   summarize_at(vars(B2_1:sti_2), median, na.rm = TRUE) %>%
#   ungroup() %>% mutate(class = "noncrop", seed = NA)

# combine and divide into train/test
train_ref_mu <- bind_rows(
  maize_wmeans, rice_wmeans, other_wmeans, noncrop_means
)
#we have weight here
#colnames(train_ref_mu2)

# training/test split
uniid <- unique(train_ref_mu$id) # unique field ids
set.seed(1)
test_ids <- sample(uniid, size = ceiling(0.2 * length(uniid))) # ids for test
train_test <- tibble(id = uniid) %>%
  mutate(usage = ifelse(id %in% test_ids, "test", "train")) # train/ref ids



train_ref_mu <- train_ref_mu %>%
  left_join(., train_test)%>%
  select(seed, id, class, usage, weight, B2_1:sti_2)
# train_ref_mu %>% group_by(class, usage) %>% count()
any(train_ref_mu$id[train_ref_mu$usage == "train"] %in%
      train_ref_mu$id[train_ref_mu$usage == "test"])

# train_ref_med <- bind_rows(maize_med, rice_med, other_med, noncrop_med)

#colnames(train_ref_mu)
# add split
train_ref_mu <- train_ref_mu %>%
  mutate(type = "mean") %>%
  select(id, class, type, seed, !!names(.))
# train_ref_med <- train_ref_med %>%
#   mutate(type = "median") %>%
#   select(id, class, type, seed, !!names(.))

any(is.na(rowSums(train_ref_mu %>% select(B2_1:sti_2))))

dat_out <- bind_rows(train_ref_mu, train_ref_med)
any(is.na(rowSums(dat_out %>% select(B2_1:sti_2))))


readr::write_csv(train_ref_mu, path = "/home/rstudio/projects/data/train/train_reference_v2.csv")



# Upload to s3
#cmd <- glue('aws s3 cp ',
#            '/home/rstudio/projects/ecaascrops/external/data/train_reference_v2.csv',' ',
#            's3://activemapper/ecaas_2021/train_reference/train_reference_v2.csv')
#system(cmd)

