# Run predictions for one year model (trained on 2021 data) for 2021

library(sf)
library(aws.s3)
library(dplyr)
library(glue)
library(parallel)
library(terra)
# library(aws.ec2metadata)
library(randomForest)
library(logr)

# Read in tile list and model

load("train_predict_tiles.rda")
ttile <- train_predict_tiles %>% pull(bmap_tile)

###### change these variables
local_dir <- ("/home/rstudio/projects/ENclass3/extdata/")
completed <- gsub("probs_|.tif", "", dir(local_dir))
ttiles <- ttile[!ttile %in% completed]
bucket <- "activemapper"
s3path <- 'ecaas_2021/RasterStack_All'
s3prefix_out <- "ecaas_2021/prob_tiles/2021"
logf <- "/home/rstudio/projects/ENclass3/extdata/rfl_2021_2"

# make sure package is built first!
load("/data/ejura_tain_rfmodel_2021_1.rda")
predvars <- ejura_tain_rfmodel_2021$keepnames
mod <- ejura_tain_rfmodel_2021$model_red
######

lf <- logr::log_open(logf)
if(file.exists(logf)) {
  file.copy(logf, paste0(logf, "bak"))
  file.remove(logf)
}

# NA and Inf counter functions
na_ct <- function(x) length(which(is.na(x)))
inf_ct <- function(x) length(which(is.infinite(x)))


starttime <- Sys.time()
msg <- glue("Starting batch of predictions at {starttime}")
cat(msg, file = logf, sep = "\n", append = TRUE)
message(msg)


o <- mclapply(ttiles, function(tile_each) {
  tile_each <- ttiles[1]
  starttime <- Sys.time()
  msg <- glue("Starting prediction for tile {tile_each} at {starttime}")
  cat(msg, file = logf, sep = "\n", append = TRUE)
  message(msg)
  #
  r_stack_s3pth <- glue('/vsis3/{bucket}/{s3path}/rststack_', tile_each,
                        '.tif')

  msg <- glue("...extracting values from images, create VIs, and cleaning")
  cat(msg, file = logf, sep = "\n", append = TRUE)
  message(msg)
  r <- terra::rast(r_stack_s3pth)

  rtemp <- r[[1]]

#  ejura_tain_rfmodel_2021$keepnames


  v <- values(r)
  v_tb <- as_tibble(v)
  #v_tb %>% View()


#  drops <- c("B2_1", "B3_1", "B4_1", "B8_1", "B5_1", "B6_1",
#             "B7_1", "B8A_1", "B11_1", "B12_1")
#  v_tb <- v_tb[, !(names(v_tb) %in% drops)]
#  names(v_tb)
#  colnames(v_tb)
  # Renames
#  v_tb <- v_tb %>%
#    rename(
#     B2_1 = B2_3,
#      B3_1 = B3_3,
#      B4_1 = B4_3,
#      B5_1 = B5_3,
#      B6_1 = B6_3,
#      B7_1 = B7_3,
#      B8A_1 = B8A_3,
#      B8_1 = B8_3,
#      B11_1 = B11_3,
#      B12_1 = B12_3,
#      Oct.1 = pla_202010.1,
#      Oct.2 = pla_202010.2,
#      Oct.3 = pla_202010.3,
#      Oct.4 = pla_202010.4,
#      Nov.1 = pla_202011.1,
#      Nov.2 = pla_202011.2,
#      Nov.3 = pla_202011.3,
#      Nov.4.= pla_202011.4
#    )

  names(v_tb)
    v_tb <- v_tb %>%
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
    ) %>% dplyr::select(all_of(predvars))

  # clean up NA and Inf
  v_tb <- v_tb %>%
    mutate_all(function(x) ifelse(is.infinite(x), NA, x)) %>%
    mutate_at(vars(all_of(predvars)), zoo::na.aggregate)  # replace NA with mean

  # v_tb %>% summarize_all(na_ct) %>% unlist() %>% sum()
  # v_tb %>% summarize_all(inf_ct) %>% unlist() %>% sum()

  # predict probabilities of each class
  msg <- glue("...making predictions at {Sys.time()}")
  cat(msg, file = logf, sep = "\n", append = TRUE)
  message(msg)
  pred <- predict(mod, v_tb, type = "prob")

  # create probability stack
  pred_stack <- do.call(c, lapply(1:ncol(pred), function(y) {
    predr <- rtemp
    values(predr) <- pred[, y]
    predr
  }))

  # write out to local
  f <- glue("{local_dir}/probs_{tile_each}.tif")
  writeRaster(pred_stack, filename = f, overwrite = TRUE)
  rm(r, v_tb, v, pred)
  gc()


#  cmd <- glue("aws s3 cp {f} ",
#              "s3://{bucket}/{s3prefix_out}/probs_{tile_each}.tif")
#  system(cmd)

  msg <- glue("Completed predictions for tile {tile_each} at {Sys.time()}")
  cat(msg, file = logf, sep = "\n", append = TRUE)
  cat("\n", file = logf, append = TRUE)
}, mc.cores = 6)

msg <- glue("Completed predictions on tiles at {Sys.time()}")
cat(msg, file = logf, sep = "\n", append = TRUE)
message(msg)


