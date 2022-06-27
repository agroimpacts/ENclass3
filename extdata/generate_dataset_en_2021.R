library(glue)
library(purrr)
library(parallel)
library(terra)
library(stringr)
library(aws.ec2metadata)
library(sf)
library(aws.s3)
library(dplyr)


system("printenv")

# Season 2 dataset
ref_crops_tiles <- st_read(
  '~/projects/geoms/tiles_nicfi.geojson'
)

#ref_crops <- st_read('/home/ubuntu/cscdc/data/catalog/d1pt7f_sample.geojson')

# ref_crops <- st_read(
#   glue('/home/rstudio/projects/ecaascrops/external/data/crop_bound/',
#        'sghana_field.geojson')
# )
ref_crops <- st_read(
  "~/projects/class1_labels/sghana_field.geojson"
)

#ref_crops %>% View()
#ref_crops %>% filter(class == "noncrop") %>% View()

ref_crops_synthetic <- st_read(
  "~/projects/class3_labels/class3_field.geojson"
)

ref_crops_synthetic$id <- seq.int(1000, (999 + nrow(ref_crops_synthetic)))
ref_crops$id <- seq.int(5000, 4999 + nrow(ref_crops))

#ref_crops_synthetic %>% View()
ref_crops <- ref_crops %>% mutate(synthetic = 0) %>% st_transform(st_crs(ref_crops_synthetic))
ref_crops_synthetic <- ref_crops_synthetic %>% mutate(synthetic = 1)

ref_crops_final <- bind_rows(ref_crops, ref_crops_synthetic)


# 4318 rows
ref_crops_final %>% View()

ref_crops_final <- do.call(rbind, lapply(1:nrow(ref_crops_final), function(n) {
  # n <- 1
  ref_crop <- ref_crops_final[n, ]
  tiles <- ref_crop$bmap_tile %>% str_split(.,';') %>% unlist()
  # ref_crop$tile <- tiles[1]
  ref_crop %>% slice(rep(1:n(), each = length(tiles))) %>%
    mutate(tile=tiles[row_number()])
}))

# all the row have only one tile
# ref_crops_final_set %>% mutate(tilelength =
#                                  nchar(ref_crops_final_set$bmap_tile) ) %>%
#   View()

#ref_crops_final$id <- seq.int(5000, (4999 + nrow(ref_crops_final)))

bucket <- 'activemapper'
s3path <- 'ecaas_2021/RasterStack_All_EN'

# stat <- sapply(ref_crops_tiles$tile[1:105], function(x){
#   aws.s3::head_object(glue('{s3path}/rststack_',x,'.tif'), bucket = bucket)})



# Read in raster stack from S3
parse_dt <- function(tile_each) {
  message(paste0('Processing tile: ', tile_each))
   #tile_each <- '1015-1063'
  # f <- paste0(
  #   "/home/ubuntu/cscdc/data/results_dataset2/tile_", tile_each, '_dataset.csv'
  # )  # file name--note I pointed it to a new folder

  f <- paste0(
    "/data/extract_new2/tile_",
    tile_each,
    '_dataset.csv'
  )  # file name--note I pointed it to a new folder

  if(file.exists(f)) {
    message(paste0(basename(f), 'already exists, skip to next tile.'))
    next
  } else {
    # Read in image
    rst_stack_s3pth <- glue('/vsis3/{bucket}/{s3path}/rststack_', tile_each,
                            '.tif')
    # rst_stack <- raster::stack(rst_stack_s3pth)

    #rst_stack <- terra::rast(raster::stack(rst_stack_s3pth) * 1)
    message(glue('Reading raster stack {tile_each} ...'))
    rst_stack <-  terra::rast(raster::readAll(raster::brick(rst_stack_s3pth)))


    #rst_stack <- terra::rast(raster::brick(rst_stack_s3pth))

    # rst_stack <- terra::rast(raster::raster(rst_stack_s3pth) * 1)

    # Filter crops that are in tile_each
    #####
    # Added by LDE: to avoid dealing with the issue where polygons overlap
    # tiles, I suggest this change, where you crop the field boundaries to the
    # extent of the raster stack. This requires first transforming the whole
    # ref_crops to UTM (only because I don't know if all tiles are in the
    # same zone, and then cropping). It doesn't crop exactly to the edge
    # (a little) larger, but at least it should allow collection from adjacent
    # tiles
    ref_crops_utm <- ref_crops_final %>%
      st_transform(., crs(rst_stack)) %>% #, proj4 = TRUE
      vect(.)
    crops <- terra::crop(ref_crops_utm, rst_stack)

    # plot(ref_crops_utm[ref_crops_utm$id == 569, ])
    # plot(crops[crops$id == 569, ], add = TRUE)
    # plot(ref_crops_utm[ref_crops_utm$id == 516, ])
    # plot(crops[crops$id == 516, ], add = TRUE)
    # plot(terra::crop(ref_crops_utm, rst_stack))
    # plot(ref_crops_utm, add = TRUE)
    # plot(terra::crop(ref_crops_utm, rst_stack), add = TRUE, col = "red")
    # plot(ext(terra::crop(ref_crops_utm, rst_stack)), add = TRUE)
    #####

    # crops <- ref_crops %>% filter(tile == tile_each)
    # crops <- crops %>% st_transform(., rst_stack %>% crs())
    #print(rst_stack)
    # Crop raster stack using the tile_each crops
    # rst_stack <- raster::crop(rst_stack,crops) #vect(as_Spatial(crops)) crops
    rst_stack <- terra::crop(rst_stack, crops) #vect(as_Spatial(crops)) crops

    dt <- do.call(rbind, lapply(unique(crops$class), function(crop_type) {
      # crop_type <- 'Maize'
      # crop_each <- crops %>%
      #   filter(class == crop_type)
      crop_each <- crops[crops$class == crop_type, ]

      # ref_frame <- as_tibble(crop_each) %>%
      #   dplyr::select(id, fid, class)
      ref_frame <- as.data.frame(crop_each) %>% dplyr::select(id, fid, class)

      # crop_each <- vect(as_Spatial(crop_each))
      crop_each_rst <- terra::rasterize(crop_each, rst_stack, field = 'id')
      # crop_each_rst <- raster::rasterize(crop_each,rst_stack)
      # names(crop_each_rst) <- 'id'

      tmp <- c(rst_stack, crop_each_rst)
      # tmp <- raster::stack(rst_stack,crop_each_rst)
      rst_mask <- terra::extract(tmp, crop_each, fun = NULL, na.rm = TRUE,
                                 df = TRUE, weights = TRUE)
      rst_vals <- rst_mask %>% group_by(ID) %>%
        mutate(id = ifelse(is.na(id), max(id, na.rm = TRUE), id)) %>%
        as.data.frame() %>%
        left_join(., ref_frame, by = "id") %>%
        dplyr::select(ID, id, fid, class, names(.)) %>%
        mutate(crop = crop_type) # some NAs now cause warnings
      # rst_vals %>% filter(ID == 19)
      return(rst_vals)
    }))

    # tmp_fname <- paste0("/home/ubuntu/cscdc/data/results_dataset/tile_", tile_each,'_dataset.csv')
    # write.csv(dt, file = tmp_fname)
    print(f)
    message('Writing csv...')
    head(dt)
    write.csv(dt, file = f)
    rm(rst_stack)
    gc()
    dt

    # Upload to S3
    # cmd <- glue('aws s3 cp ',
    #             f,' ',
    #             's3://{bucket}/ecaas_2021/train_reference_tile/tile_',tile_each,'_dataset.csv')
    # system(cmd)
  }
}


# message('Create the Training Datasest......')
#dataset <- lapply(unique(ref_crops$tile), possibly(parse_dt, otherwise = NULL))

dataset <- lapply(unique(ref_crops_final$tile)[51:102], parse_dt)

