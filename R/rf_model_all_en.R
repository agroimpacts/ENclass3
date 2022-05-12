# Train model:
# used to train all 3 models:
# 1. Year 2 model
# 2. Year 1 + 2 model
require(caTools)
library(randomForest)
library(dplyr)
library(aws.s3)
library(ggplot2)
library(patchwork)


#read in train set on s3
# year 2 only model
# tref_in <- "ecaas_2021/train_reference/train_reference_v2.csv"
# mod_out <- here::here("inst/extdata/trained_rf_y2.rda")
# dataset


train_ref <- readr::read_csv("/home/rstudio/projects/data/train/train_reference_v2.csv") %>%
  mutate(crop = factor(case_when(
  class == "Maize" ~ 1,
  class == "Rice" ~ 2,
  class == "Other" ~ 3,
  class == "noncrop" ~ 4
))) %>% select(id, class, crop, !!names(.))


#------------------------------------------------------------------------------#
# Year 2 only model
# dataset
train_ref_y2 <- train_ref #%>% filter(year == 2021)

train_ref_y2 %>% group_by(class) %>% count()
any(train_ref_y2$id[train_ref_y2$usage == "train"] %in%
      train_ref_y2$id[train_ref_y2$usage == "test"])

train_ref_y2l <- lapply(c(10, 20, 30, 40), function(x) {
  set.seed(x)
  noncrop <- train_ref_y2 %>%
    filter(class == "noncrop") %>%
    sample_n(421, replace = FALSE)
  rice <- train_ref_y2 %>%
    filter(class == "Rice") %>%
    sample_n(421, replace = FALSE)
  other <- train_ref_y2 %>%
    filter(class == "Other") %>%
    sample_n(421)

  bind_rows(
    train_ref_y2 %>% filter(class == "Maize"),
    rice, other, noncrop
  ) %>% mutate(seed_y2 = x)
}) %>% do.call(rbind, .)

# train_refl_y2l %>% group_by(usage, class, seed_y2) %>% count() %>%
#   arrange(seed_y2) %>% View()

# checking not all in--they aren't
# all((train_refl_y2l %>% filter(class == "Rice" & seed_y2 == 10) %>% pull(id)) %in%
#   (train_refl_y2l %>% filter(class == "Rice" & seed_y2 == 20) %>% pull(id)))
# train_refl_y2l %>% filter(class == "Maize" & usage == "test") %>% View()

# Split data
colnames(train_ref_y2l)
models_year2 <- lapply(c(10, 20, 30, 40), function(x) {  # x <- 10

  train <- train_ref_y2l %>%
    filter(seed_y2 == x & usage == "train") %>%
    select(-id, -class, -seed, -weight, -usage, -seed_y2, -type)
  test <- train_ref_y2l %>%
    filter(seed_y2 == x & usage == "test") %>%
    select(-id, -class, -seed, -weight, -usage, -seed_y2, -type)
#models_year2 <- lapply(c(10, 20, 30, 40), function(x) {  # x <- 10

#  train <- train_ref_y2l %>%
#    filter(seed_y2 == x & usage == "train") %>%
#    select(-id, -class, -seed, -weight, -usage, -year)
#  test <- train_ref_y2l %>%
#    filter(seed_y2 == x & usage == "test") %>%
#    select(-id, -class, -seed, -weight, -usage, -year)

  # Train
  message(glue("Training on set {x}"))
  set.seed(123)
  mod <- randomForest(crop ~ ., data = train, ntree = 1000,
                      importance = TRUE)

  colnames(mod)
  mod %>% View()
  # Evaluate
  pred <- predict(mod, newdata = test %>% dplyr::select(-c(crop)))
  cm <- table(test$crop, pred)
  # sum(diag(cm)) / sum(cm)

  #
  imp <- mod$importance
  nms <- rownames(imp)
  imp_tbl <- as_tibble(imp) %>% mutate(name = nms) %>%
    arrange(-MeanDecreaseAccuracy) %>%
    mutate(order = 1:n()) %>%
    rename(AccDec = MeanDecreaseAccuracy)

  message(glue("Finished set {x}"))

  return(list("model" = mod, "cmatrix" = cm, "imp_table" = imp_tbl))

})

names(models_year2) <- c(10, 20, 30, 40)

sapply(models_year2, function(x) sum(diag(x$cmatrix)) / sum(x$cmatrix))

ps <- lapply(models_year2, function(x) {
  p <- ggplot(x$imp_table) +
    geom_point(aes(AccDec, factor(order))) +
    scale_y_discrete(labels = toupper(x$imp_table$name)) +
    ylab("Variable") + xlab("Mean Decrease in Accuracy") +
    theme_linedraw() +
    theme(axis.text = element_text(size = 7),
          axis.title = element_text(size = 7))
})

# pretty clear break at 0.01 decrease in accuracy
(ps[[1]] + ps[[2]]) / (ps[[3]] + ps[[4]])

# count how many times variables appeared across the 4 sets. Keep those in 2 or
# more

brk <- 0.01
keep_nmsl <- lapply(1:length(models_year2), function(x) {
  keep_nms <- models_year2[[x]]$imp_table %>%
    filter(AccDec >= brk) %>%
    select(name) %>%
    mutate(set = 1)
}) %>% do.call(rbind, .)
keep_nms <- keep_nmsl %>% group_by(name) %>% count() %>%
  filter(n > 1) %>% pull(name)

# final model will be just one of these four datasets, using the keep names
set.seed(1)
dset <- train_ref_y2l %>%
  filter(seed_y2 == sample(c(10, 20, 30, 40), 1)) %>%
  select(-class, -seed, -weight) %>%
  select(id, crop, !!keep_nms, usage)#, seed_y2)
# unique(dset$seed_y2)  # 10

any(dset$id[dset$usage == "train"] %in%
      dset$id[dset$usage == "test"])

# set.seed(10)
# samp <- sample.split(dset$crop, SplitRatio = 0.8)
train2 <- dset %>% filter(usage == "train") %>% select(-id, -usage)
test2 <- dset %>% filter(usage == "test") %>% select(-id, -usage)
# train %>% tidyr::drop_na()

# reduced model performs better!
set.seed(123)
modr <- randomForest(crop ~ ., data = train2, ntree = 1000,
                     importance = TRUE)
predr <- predict(modr, newdata = test2 %>% dplyr::select(-c(crop)))
cmr <- table(test2$crop, predr)
sum(diag(cmr)) / sum(cmr)

impr <- modr$importance
nmsr <- rownames(impr)
imp_tblr <- as_tibble(impr) %>% mutate(name = nmsr) %>%
  arrange(-MeanDecreaseAccuracy) %>%
  mutate(order = 1:n()) %>%
  # mutate(Variable = nms) %>%
  rename(AccDec = MeanDecreaseAccuracy)

p <- ggplot(imp_tblr) +
  geom_point(aes(AccDec, factor(order))) +
  scale_y_discrete(labels = toupper(imp_tblr$name)) +
  ylab("Variable") + xlab("Mean Decrease in Accuracy") +
  theme_linedraw() +
  theme(axis.text = element_text(size = 7),
        axis.title = element_text(size = 7))


ejura_tain_rfmodel_2021 <- list(
  "model" = models_year2$`10`$model,
  "cmatrix" = models_year2$`10`$cm,
  "imp_table" = models_year2$`10`$imp_tbl,
  "model_red" = modr, "cmatrix_red" = cmr,
  "keepnames" = keep_nms
)

mod_out <- "/data/ejura_tain_rfmodel_2021_1.rda"
save(ejura_tain_rfmodel_2021, file = mod_out)



