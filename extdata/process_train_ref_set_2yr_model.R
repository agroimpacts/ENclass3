# Process model results from AWS instance used to process data
# Season 1 and Season 2

library(dplyr)
library(sf)
library(glue)
library(tidyr)
library(ecaascrops)
library(aws.s3)
library(here)
# library(parallel)

# get train tile IDs
data("train_predict_tiles")
ttile <- train_predict_tiles %>%
  filter(usage == "train") %>% pull(bmap_tile)

# get data from instance
# dest <- "/home/rstudio/projects/cscdc/data/dataset/"  # season 1 data
# dest2 <- "/home/rstudio/projects/ecaascrops_untracked/external/data/extract/" # season 2

prefix1 <- "ecaas/Result_Dataset/tile_"  # season 1 data
prefix2 <- "ecaas_2021/train_reference_tile/tile_" # season 2

# # read in y1 data
# dat_y1 <- lapply(dir(dest, full.names = TRUE), function(x) {
#   readr::read_csv(x) %>%
#     mutate(tile = gsub("tile_|_dataset.csv", "", basename(x))) %>%
#     select(ID, id, fid, tile, !!names(.))
# })

# Year 1 data, dropping the missing tiles (see generate_dataset_2020.R)
drops <- c("1012-1066", "1017-1062")
dat_y1 <- lapply(ttile[!ttile %in% drops], function(x) {  # x <- ttile[1]
  # glue("{prefix1}{x}_dataset.csv")
  s3read_using(
    readr::read_csv,
    object = glue("{prefix1}{x}_dataset.csv"), # change prefix here!!!
    bucket = "activemapper"
  ) %>% mutate(tile = x) %>%
    select(ID, id, fid, tile, !!names(.))
})
dat_y1f <- do.call(rbind, dat_y1) %>%
  select(-crop, -...1) %>%
  mutate(year = 2020) %>%
  select(ID, id, fid, tile, class, year, weight, !!names(.))

#read in y2 data
# dat_y2 <- lapply(dir(dest2, full.names = TRUE), function(x) {
#   readr::read_csv(x) %>%
#     mutate(tile = gsub("tile_|_dataset.csv", "", basename(x))) %>%
#     select(ID, id, fid, tile, !!names(.))
# })

# Year 2 data, no missing tiles
dat_y2 <- lapply(ttile, function(x) {  # x <- ttile[1]
  # glue("{prefix1}{x}_dataset.csv")
  s3read_using(
    readr::read_csv,
    object = glue("{prefix2}{x}_dataset.csv"),  # change prefix here!!!
    bucket = "activemapper"
  ) %>% mutate(tile = x) %>%
    select(ID, id, fid, tile, !!names(.))
})
dat_y2f <- do.call(rbind, dat_y2) %>%
  dplyr::select(-crop, -...1) %>%
  mutate(year = 2021) %>%
  dplyr::select(ID, id, fid, tile, class, weight, !!names(.))

drops <- c("pla_20200608.1","pla_20200608.2", "pla_20200608.3",
           "pla_20200608.4", "pla_202012.1", "pla_202012.2", "pla_202012.3",
           "pla_202012.4", "pla_202101.1", "pla_202101.2", "pla_202101.3",
           "pla_202101.4")
#, "ndvi_1", "rg1_ndvi_1", "rg2_ndvi_1", "gcvi_1", "rg1_gcvi_1",
# "rg2_gcvi_1", "mtci_1", "mtci2_1", "reip_1", "nbr1_1", "nbr2_1", "ndti_1",
# "crc_1", "sti_1"

dat_y1f <- dat_y1f[, !(names(dat_y1f) %in% drops)]

drops2 <- c("B2_1", "B3_1", "B4_1", "B8_1", "B5_1", "B6_1",
            "B7_1", "B8A_1", "B11_1", "B12_1")
# , "ndvi_1", "rg1_ndvi_1", "rg2_ndvi_1", "gcvi_1", "rg1_gcvi_1",
# "rg2_gcvi_1", "mtci_1", "mtci2_1", "reip_1", "nbr1_1", "nbr2_1", "ndti_1",
# "crc_1", "sti_1"
dat_y2f <- dat_y2f[, !(names(dat_y2f) %in% drops2)]

dat_y2f <- dat_y2f %>%
  rename(
    B2_1 = B2_3,
    B3_1 = B3_3,
    B4_1 = B4_3,
    B5_1 = B5_3,
    B6_1 = B6_3,
    B7_1 = B7_3,
    B8A_1 = B8A_3,
    B8_1 = B8_3,
    B11_1 = B11_3,
    B12_1 = B12_3
  )

dat <- rbind(dat_y1f, dat_y2f)

dat <- dat %>%
  rename(
    Oct.1 = pla_202010.1,
    Oct.2 = pla_202010.2,
    Oct.3 = pla_202010.3,
    Oct.4 = pla_202010.4,
    Nov.1 = pla_202011.1,
    Nov.2 = pla_202011.2,
    Nov.3 = pla_202011.3,
    Nov.4.= pla_202011.4
  )

names(dat)

# add Sentinel-2 indices
dat <- dat %>%
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
dat %>% group_by(id) %>%
  summarize_at(vars(B2_1:sti_2), na_ct) %>%
  summarize_at(vars(B2_1:sti_2), max) %>% unlist()
dat %>% group_by(id) %>%
  summarize_at(vars(B2_1:sti_2), inf_ct) %>%
  summarize_at(vars(B2_1:sti_2), max) %>% unlist()
dat %>% group_by(id) %>%
  filter(is.infinite(B5_1))


# filer out ids = -Inf
dat2 <- dat %>% filter(id != -Inf)

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
datf <- datf %>% tidyr::drop_na(vh_1)
# should returns FALSE after fixing
any(is.na(rowSums(datf %>% select(B2_1:sti_2))))
# doesn't as there are some Planet variables that are NA, so drop
# datf %>% slice(which(is.na(rowSums(datf %>% select(B2_1:sti_2))))) %>% View()
datf <- datf %>%
  slice(-which(is.na(rowSums(datf %>% select(B2_1:sti_2)))))

# unique(datf$tile) %in% ttile
##########
# maize subsets
# maize_wmeans <- lapply(seq(150, 150, by = 50), function(x) {  # x <- 1
#   set.seed(x)
#   datf %>% filter(class == "Maize") %>%
#     group_by(id) %>%
#     filter(weight >= 0.5) #%>% count() #%>% pull(n) %>% range(.)
#     # sample_n(size = ceiling(n() * 0.1), replace = TRUE) %>%  # don't sample
#     summarize_at(vars(sti_2:weight), weighted.mean, w = quo(weight), mean,
#                  na.rm = TRUE) %>%
#     mutate(seed = x) %>%
#     select(seed, id, !!names(.))
# }) %>% do.call(rbind, .) %>%
#   mutate(class = "Maize")


# reorganize to have weight at end (checked against maize_wmeans original, see
# lines 220, 224, 233:238)
datf <- datf %>% select(ID:class, year, B2_1:ncol(.), weight) #%>% names() %in%
# names(datf)

# Don't sample maize because there are 992 fields
# checked that year variable didn't mess things up by running maize_wmeans2
maize_wmeans <- datf %>%
  # maize_wmeans2 <- datf %>%  # ran check here and below after adding year var
  filter(class == "Maize") %>%
  # group_by(id) %>%
  group_by(id, year) %>%
  filter(weight >= 0.5) %>% # count() #%>% pull(n) %>% range(.)
  # summarize_at(vars(sti_2:weight), weighted.mean, w = quo(weight),
  #              na.rm = TRUE) %>%  # original version - checked that is same
  summarize_at(vars(B2_1:weight), weighted.mean, w = quo(weight), #mean,
               na.rm = TRUE) %>%
  mutate(year = round(year)) %>%
  mutate(seed = 0) %>%
  select(seed, id, !!names(.)) %>%
  mutate(class = "Maize")

# Checks - all fine
# all(maize_wmeans %>% arrange(id) %>% pull(sti_2) ==
#   maize_wmeans2 %>% arrange(id) %>% pull(sti_2))
# maize_wmeans2 %>% group_by(year) %>% count()
# datf %>% filter(class == "Maize") %>%
#   group_by(id, year) %>% count() %>%
#   group_by(year) %>% count()

##########
# rice subsets  - resample three times to boost
# checked that year variable didn't mess things up by running rice_wmeans2
rice_wmeans <- lapply(seq(40, 120, by = 40), function(x) {  # x <- 1
  # rice_wmeans2 <- lapply(seq(40, 120, by = 40), function(x) {  # x <- 1
  set.seed(x)
  datf %>% filter(class == "Rice") %>%
    # group_by(id) %>%
    group_by(id, year) %>%
    filter(weight > 0.5) %>% #count()
    sample_n(size = ceiling(n() * 0.33), replace = TRUE) %>%
    # summarize_at(vars(sti_2:weight), weighted.mean, w = quo(weight), mean,
    #              na.rm = TRUE) %>%
    summarize_at(vars(B2_1:weight), weighted.mean, w = quo(weight), #mean,
                 na.rm = TRUE) %>%
    mutate(seed = x) %>%
    select(seed, id, !!names(.))
}) %>% do.call(rbind, .) %>%
  mutate(class = "Rice")

# Checks - all fine
# all(rice_wmeans %>% arrange(id) %>% pull(sti_2) ==
#   rice_wmeans2 %>% arrange(id) %>% pull(sti_2))
# rice_wmeans2 %>% group_by(year) %>% count()
# rice_wmeans %>% count()
# datf %>% filter(class == "Rice") %>%
#   group_by(id, year) %>% count() %>%
#   group_by(year) %>% count()
# count() %>% pull(n) %>% hist()

##########
# other subsets - resample twice to boost
# checked that year variable didn't mess things up by running other_wmeans2
other_wmeans <- lapply(seq(150, 300, by = 150), function(x) {  # x <- 1
  # other_wmeans2 <- lapply(seq(150, 300, by = 150), function(x) {  # x <- 1
  set.seed(x)
  datf %>% filter(class == "Other") %>%
    # group_by(id) %>%
    group_by(id, year) %>%
    filter(weight > 0.5) %>%
    sample_n(size = ceiling(n() * 0.5), replace = TRUE) %>%
    # summarize_at(vars(sti_2:weight), weighted.mean, w = quo(weight), mean,
    #              na.rm = TRUE) %>%
    summarize_at(vars(B2_1:weight), weighted.mean, w = quo(weight), #mean,
                 na.rm = TRUE) %>%
    mutate(seed = x) %>%
    select(seed, id, !!names(.))
}) %>% do.call(rbind, .) %>%
  mutate(class = "Other")

# Checks - all fine
# all(other_wmeans %>% arrange(id) %>% pull(sti_2, B2_1) ==
#   other_wmeans2 %>% arrange(id) %>% pull(sti_2, B2_1))
# other_wmeans2 %>% group_by(year) %>% count()
# other_wmeans %>% count()
# datf %>% filter(class == "Other") %>%
#   group_by(id, year) %>% count() %>%
#   group_by(year) %>% count()

# noncrop
# datf %>% filter(class == "noncrop") %>%
#   distinct(id) %>% count()

# sample size is mean of other samples
# ssize <- round(
#   mean(c(nrow(maize_wmeans), nrow(rice_wmeans), nrow(other_wmeans)))
# )

# number of unique samples is 1032, which is basically same size, so no sampling
# needed
# set.seed(1)

# checked that year variable didn't mess things up by running noncrop_means2
noncrop_means <- datf %>% filter(class == "noncrop") %>%
  # noncrop_means2 <- datf %>% filter(class == "noncrop") %>%
  # group_by(id) %>%
  group_by(id, year) %>%
  summarize_at(vars(B2_1:sti_2), mean, na.rm = TRUE) %>%
  ungroup() %>% mutate(class = "noncrop", seed = NA) # %>%
# sample_n(size = ssize)
any(is.na(rowSums(noncrop_means %>% select(B2_1:sti_2))))

# all(noncrop_means %>% arrange(id) %>% pull(sti_2, B2_1) ==
#   noncrop_means2 %>% arrange(id) %>% pull(sti_2, B2_1))
# noncrop_means2 %>% group_by(year) %>% count()
# noncrop_med <- datf %>% filter(class == "noncrop") %>%
#   group_by(id) %>%
#   summarize_at(vars(B2_1:sti_2), median, na.rm = TRUE) %>%
#   ungroup() %>% mutate(class = "noncrop", seed = NA)

# combine and divide into train/test
train_ref_mu <- bind_rows(
  maize_wmeans, rice_wmeans, other_wmeans, noncrop_means
)

# training/test split (for two year model only)
uniid <- unique(train_ref_mu$id) # unique field ids
set.seed(1)
test_ids <- sample(uniid, size = ceiling(0.2 * length(uniid))) # ids for test
train_test <- tibble(id = uniid) %>%
  mutate(usage = ifelse(id %in% test_ids, "test", "train")) # train/ref ids
train_ref_mu <- train_ref_mu %>%
  left_join(., train_test)
# %>%
#   select(seed, id, class, usage, B2_1:ncol(.))
train_ref_mu %>% group_by(class, usage) %>% count()
train_ref_mu %>% group_by(class, usage, year) %>% count() %>% arrange(year)
any(train_ref_mu$id[train_ref_mu$usage == "train"] %in%
      train_ref_mu$id[train_ref_mu$usage == "test"])

# train_ref_med <- bind_rows(maize_med, rice_med, other_med, noncrop_med)
# add split
# train_ref_mu <- train_ref_mu %>%
#   mutate(type = "mean") %>%
#   select(id, class, type, seed, !!names(.))
# # train_ref_med <- train_ref_med %>%
# #   mutate(type = "median") %>%
# #   select(id, class, type, seed, !!names(.))
#
# any(is.na(rowSums(train_ref_mu %>% select(B2_1:sti_2))))
#
# dat_out <- bind_rows(train_ref_mu, train_ref_med)
# any(is.na(rowSums(dat_out %>% select(B2_1:sti_2))))

train_ref_mu <- train_ref_mu %>% select(id, class, year, seed, !!names(.))

# f <- "/home/rstudio/projects/ecaascrops_untracked/external/data/train_reference_y1y2.csv"
# s3f <- "s3://activemapper/ecaas_2021/train_reference/train_reference_y1y2.csv"
readr::write_csv(
  train_ref_mu,
  file = here("inst/extdata/train_test_y1y2.csv")
)

# Upload to s3
s3write_using(train_ref_mu, FUN = readr::write_csv,
              object = "ecaas_2021/train_reference/train_test_y1y2.csv",
              bucket = "activemapper")
