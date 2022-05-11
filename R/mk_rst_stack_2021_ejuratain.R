library(sf)
library(aws.s3)
library(dplyr)
library(tidyverse)
library(glue)
library(purrr)
library(parallel)
library(terra)
library(stringr)
library(aws.ec2metadata)

bucket <- 'activemapper'
s3path <- 'ecaas_2021/EN_RasterStack'

# Gather Planet catalogs (already in nicfi tiles)
## PlanetScope B G R NIR Alpha
items <- get_bucket(bucket = 'activemapper',
                    prefix = 'ecaas_2021/nicfi',
                    max = Inf)
keys <- lapply(c(1: length(items)), function(i) {
  key <- items[[i]]$Key
})
planet_catalog <- unlist(keys) %>% data.frame(path = .) %>%
  mutate(
    tile = gsub("tile", "",
                stringr::str_extract(path, "\\d[0-9]{1,4}+\\-+\\d[0-9]{3,4}")),
    month = stringr::str_extract(path, "(?<=analytic_)(.*?)(?=_mosaic)")
  ) %>% mutate(s3_path = paste0('/vsis3/activemapper/',path))


planet_catalog %>% View()

# planet_catalog %>% nrow()
# planet_catalog %>% filter(grepl('tif$',s3_path)) %>% nrow()
planet_catalog <- planet_catalog %>%
  filter(grepl('tif$',s3_path))

# select only October and November this time
planet_catalog <- planet_catalog %>%
  filter(month %in% c("2021-06", "2021-07", "2021-08", "2021-09",
                      "2021-10", "2021-11","2021-12","2022-01"))


## Sentinel1_harmonic catalogs (already in nicfi tiles)
items <- get_bucket(bucket = 'activemapper',
                    prefix = 'imagery/sentinel1/level3/2021',
                    max = Inf)
keys <- lapply(c(1: length(items)), function(i) {
  key <- items[[i]]$Key
})
s1_catalog <- unlist(keys) %>% data.frame(path = .) %>%
  mutate(tile = gsub(
    "tile", "",
    stringr::str_extract(path, "\\d[0-9]{1,4}+\\-+\\d[0-9]{3,4}"))
  ) %>% mutate(s3_path = paste0('/vsis3/activemapper/',path))
s1_catalog <- s1_catalog[-c(1),]
s1_catalog %>% View()

# Make sentinel2 from individual band to band groups (B2348, B5678A1112)
# for each season meanwhile maintains the original sentinel tile at the moment.

s2_tile <- c("NWN", "NWP", "NXN", "NXP",
             "NYN", "NYP", "PWQ", "PWR",
             "PXQ", "PYQ")
s2_midday <- c('0617','1216')
s2_bandgrp1 <- c("B2", "B3", "B4", "B8")
s2_bandgrp2 <- c("B5", "B6","B7", "B8A","B11", "B12")
items <- get_bucket(bucket = 'activemapper',
                    prefix = 'imagery/sentinel2/level3/2021',
                    max = Inf)

keys <- lapply(c(1: length(items)), function(i) {
  key <- items[[i]]$Key
})

s2_catalog <- unlist(keys) %>% data.frame(path = .) %>%
  mutate(s3_path = paste0('/vsis3/activemapper/',path))

# add columns including band, season and midday of season.
s2_catalog <- s2_catalog %>%
  filter(grepl('tif$',s3_path)) %>%
  filter(grepl('FRC',s3_path)) %>% # only keep data instead of mask
  mutate(
    tile = gsub("tile", "",
                stringr::str_extract(path, "(?<=T30)(.*?)(?=_C_V1)")),
    midday = stringr::str_extract(path, "(?<=SENTINEL2X_2021)(.*?)(?=-0000)"),
    band = stringr::str_extract(path, "(?<=FRC_)(.*?)(?=.tif)")
  ) %>% mutate(
    season = ifelse(
      midday == "0617", "s1",
      ifelse(midday=="1216", "s2", NA))
    )

s2_catalog %>%  View()
# make stack in sentinel tiles and push them to S3
season_list <- c("s1","s2")

# crop sentinel 2 tiles by entire nicfi area to reduce tif size
nicfi <- st_read(glue('/home/rstudio/projects/ecaascrops/external/data/',
                      'crop_bound/','nicfi_dissolved.geojson'))

lapply(s2_tile[7:10], function(this_tile) {
  message("working on tile: ", this_tile)

  lapply(season_list, function(this_season) {

    B2348_path <- s2_catalog %>%
      filter(tile==this_tile & season==this_season) %>%
      filter(band %in% s2_bandgrp1)%>%
      pull(s3_path)


    # make B2345 file for a season
    B2348 <- lapply(B2348_path, function(s3_path){
      raster::stack(s3_path)
    }) %>% raster::stack()

    names(B2348) <- c("B2","B3","B4","B8")

    nicfi_newcrs <- nicfi %>% st_transform(crs=raster::crs(B2348))
    new_tile_bound <- raster::extent(B2348) %>% as('SpatialPolygons') %>%
      st_as_sf() %>%
      st_set_crs(raster::crs(B2348)) %>%
      st_intersection(nicfi_newcrs)

    B2348 <- raster::crop(B2348,new_tile_bound)

    message("writing raster B2348 for season: ", this_season)
    fnm <- glue("/home/rstudio/projects/ecaascrops/external/data/",
                "Stack_S2_2021/Sentinel2_{this_tile}_B2348_{this_season}.tif")
    raster::writeRaster(B2348, filename = f, overwrite=TRUE)

    # make B5678A111 file for a season
    B5678A1112_path <- s2_catalog %>%
      filter(tile==this_tile & season== this_season) %>%
      filter(band %in% s2_bandgrp2) %>%
      pull(s3_path)

    B5678A1112 <- lapply(B5678A1112_path, function(s3_path){
      raster::stack(s3_path)
    }) %>% raster::stack()
    names(B5678A1112) <- c("B5","B6","B7","B8A","B11","B12")

    B5678A1112 <- raster::crop(B5678A1112,new_tile_bound)
    message("writing raster B5678A1112 for season: ", this_season)
    fnm <- glue("/home/rstudio/projects/ecaascrops/external/data/",
                "Stack_S2_2021/Sentinel2_{this_tile}_B5678A1112_",
                "{this_season}.tif")

    raster::writeRaster(B5678A1112, filename = f, overwrite=TRUE)
    rm(B2348, B5678A1112)
    gc()
  })

})

# push sentinel2 tiles band groups to s3
s2_stack_list <- list.files(glue('/home/rstudio/projects/ecaascrops/',
                                 'external/data/Stack_S2_2021/'))

lapply(s2_stack_list[1:60], function(this_file){
  cmd <- glue(
    "aws s3 cp ",
    "/home/rstudio/projects/ecaascrops/external/data/Stack_S2_2021/{this_file}",
    " s3://{bucket}/ecaas_2021/Sentinel2_Stack/{this_file}")
  system(cmd)
})


# make sentinel stacks in nicfi tiles

# method 1 (maybe slow and fail during the mosaic):
# mosaic these sentinel tiles together and clip by nicfi tiles, then push to S3
# gave up method 1.

# method 2
# use each nicfi tile to cut each sentinel 2 tile stack, and mosaic the
# pieces if it is not null
items <- get_bucket(bucket = 'activemapper',
                    prefix = 'ecaas_2021/Sentinel2_Stack',
                    max = Inf)

keys <- lapply(c(1: length(items)), function(i) {
  key <- items[[i]]$Key
})

s2_catalog <- unlist(keys) %>% data.frame(path = .) %>%
  mutate(s3_path = paste0('/vsis3/activemapper/',path))

s2_catalog <- s2_catalog %>%
  filter(grepl('tif$', s3_path))

s2_catalog <- s2_catalog %>%
  mutate(file_name = basename(path)) %>%
  mutate(
    tile = stringr::str_extract(file_name, "(?<=Sentinel2_)(.*?)(?=_B)"),
    season = paste0(
      "s",
      stringr::str_extract(file_name, "(?<=_s)(.*?)(?=.tif)")
    ),
    bandgrp = paste0("B",stringr::str_extract(file_name, "(?<=_B)(.*?)(?=_s)"))
  )

ref_crops_tiles <- st_read(
  '/vsis3/activemapper/ecaas/labels/tiles_nicfi.geojson'
)

season_list <- c("s1","s2","s3")
band_grp_list <- c("B2348", "B5678A1112")

lapply(ref_crops_tiles$tile[76:105], function(this_tile) {
  message("working on tile: ", this_tile)
  lapply(band_grp_list, function(this_grp) {
    lapply(season_list, function(this_season) {

      # pull all sentinel tiles (images)
      sentinel_list <- s2_catalog %>%
        filter(season == this_season & bandgrp == this_grp) %>%
        pull(s3_path)

      # use tile to cut each sentinel 2 tiles (images)
      # if not intersect, return null
      tile_chips <- lapply(sentinel_list, function(this_file){
        r <- raster::stack(this_file)
        r_bbox <- as(raster::extent(r), "SpatialPolygons") %>%
          st_as_sf() %>%
          st_set_crs(raster::crs(r))

        nicfi_tile <- ref_crops_tiles %>%
          filter(tile == this_tile) %>%
          st_as_sf() %>%
          st_transform(crs = raster::crs(r))

        check_intersect <- st_intersects(nicfi_tile, r_bbox)


        if(is.na(as.numeric(check_intersect))){
          return()
        } else {
          raster::crop(r, nicfi_tile)
        }
      })

      # remove nulls
      tile_chips <- tile_chips[!sapply(tile_chips,is.null)]

      # mosaic
      if (length(tile_chips) >1){
        tile_chips$fun <- mean
        tile_mosaiced <- tile_chips %>% do.call(raster::mosaic, .)
      } else {
        tile_mosaiced <- tile_chips[[1]]
      }
      # write to local
      message(glue("writing {this_tile} {this_grp} {this_season} to disk"))
      fnm <- glue(
        "/home/rstudio/projects/ecaascrops/external/data/",
        "Stack_S2_2021/Sentinel2_{this_tile}_{this_grp}_{this_season}.tif"
      )
      raster::writeRaster(tile_mosaiced, filename = fnm, overwrite=TRUE)
      rm(tile_mosaiced)
      gc()

    })
  })

})


# push sentinel_nicfi to s3
s2_stack_list <- list.files(
  glue('/home/rstudio/projects/ecaascrops/external/data/Stack_S2_2021/')
)
cmd <- glue(
  'aws s3 cp ',
  '/home/rstudio/projects/ecaascrops/external/data/Stack_S2_2021/',
  s2_stack_list[1],' ',
  's3://{bucket}/ecaas_2021/Sentinel2_nicfi/', s2_stack_list[1]
)
lapply(s2_stack_list[1:630], function(this_file){
  cmd <- glue(
    'aws s3 cp ',
    '/home/rstudio/projects/ecaascrops/external/data/Stack_S2_2021/',
    this_file,' s3://{bucket}/ecaas_2021/Sentinel2_nicfi/', this_file
  )
  system(cmd)
})

## Sentinel2_syntheses (nicfi tile)
items <- get_bucket(bucket = 'activemapper',
                    prefix = 'ecaas_2021/Sentinel2_nicfi',
                    max = Inf)
keys <- lapply(c(1: length(items)), function(i) {
  key <- items[[i]]$Key
})
s2_catalog <- unlist(keys) %>% data.frame(path = .) %>%
  mutate(
    tile = gsub(
      "tile", "",
      stringr::str_extract(path, "\\d[0-9]{1,4}+\\-+\\d[0-9]{3,4}")
    )
  ) %>% mutate(s3_path = paste0('/vsis3/activemapper/',path))
s2_catalog <- s2_catalog[!(is.na(s2_catalog$tile)), ]

#tile_each <- '1008-1066'
# create S1, S2, and Planet stack
s3_fpth <- glue('{s3path}/rststack_', tile_each,'.tif')
s3_status <- aws.s3::head_object(s3_fpth, bucket = bucket)

ref_crops_tiles <- st_read(
  '/vsis3/activemapper/ecaas/labels/tiles_nicfi.geojson')

lapply(ref_crops_tiles$tile[1:39], function(tile_each){

  message(paste0('Generating raster stack for tile : ', tile_each))

  # Read Sentinel2 10m images as reference data for later resample.
  s2_syntheses_10m <- s2_catalog %>%
    filter(tile == tile_each & path %>% str_detect(.,'B2348')==T)
  s2_syntheses_10m <- do.call(
    c,
    lapply(s2_syntheses_10m$s3_path, function(each) {
      rst <- terra::rast(raster::stack(each))
    })
  )

  # terra::rast(raster::raster("/vsis3/activemapper/ecaas/Sentinel2_nicfi_fix/Sentinel2_B2348_1008-1066_s1.tif"))

  # Read in Sentinel-2
  s2_syntheses <- s2_catalog %>% filter(tile == tile_each)
  s2_syntheses <- do.call(c, lapply(s2_syntheses$s3_path, function(each) {
    rst <- rast(raster::stack(each))
    resample(rst, s2_syntheses_10m[[1]])
  }))
  names(s2_syntheses) <- c(
    "B2_1", "B3_1", "B4_1", "B8_1",
    "B2_2", "B3_2", "B4_2", "B8_2",
    "B2_3", "B3_3", "B4_3", "B8_3",
    "B5_1", "B6_1", "B7_1", "B8A_1","B11_1", "B12_1",
    "B5_2", "B6_2", "B7_2", "B8A_2","B11_2", "B12_2",
    "B5_3", "B6_3", "B7_3", "B8A_3","B11_3", "B12_3"
  )

  # Read in Planetscope
  planet <- planet_catalog %>% filter(tile == tile_each)
  planet <- do.call(c, lapply(planet$s3_path, function(each) {
    rst <- terra::rast(raster::stack(each))[[1:4]]
    terra::resample(rst,s2_syntheses_10m[[1]])
  }))
  names(planet) <- c(
    "pla_202110.1", "pla_202110.2", "pla_202110.3", "pla_202110.4",
    "pla_202111.1", "pla_202111.2", "pla_202111.3", "pla_202111.4"
  )

  # Read in Sentinel-1 harmonic result
  s1_harm_all <- s1_catalog %>% filter(tile == tile_each)
  s1_harm_all <- do.call(c, lapply(s1_harm_all$s3_path, function(each) {
    rst <- terra::rast(raster::stack(each))
    terra::resample(rst, s2_syntheses_10m[[1]])
  }))
  names(s1_harm_all) <- c(paste0('vh_', 1:6),
                          paste0('vv_', 1:6))

  # Stack raster data together
  rst_stack <- c(s2_syntheses, planet, s1_harm_all)
  # Save it to local
  rst_stack_nam <- paste0(
    '/home/rstudio/projects/ecaascrops/external/data/tmp_trash/rststack_',
    tile_each, '.tif'
  )

  writeRaster(rst_stack,rst_stack_nam)

  # Push to S3
  cmd <- glue(
    "aws s3 cp {rst_stack_nam} ",
    "s3://{bucket}/{s3path}/rststack_{tile_each}.tif"
  )
  system(cmd)

  # Remove local file
  file.remove(rst_stack_nam)

  rm(rst_stack)
  gc()
})

