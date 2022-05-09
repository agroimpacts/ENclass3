library(aws.s3, quietly = T)
library(stringr, quietly = T)
library(dplyr, quietly = T)
library(parallel, quietly = T)
library(sf, quietly = T)
library(lubridate, quietly = T)
library(ggplot2, quietly = T)

# sentinel 1 level3 images in s3 bucket
s1l3_items <- get_bucket(bucket = 'activemapper',
                         prefix = 'imagery/sentinel1/level3/2021',
                         max = Inf)

# create dataframe with file names and tile id
s1l3_ctl <- do.call(rbind, lapply(c(2:length(s1l3_items)), function(i) {
  nm <- basename(s1l3_items[[i]]$Key)
  tile_id <- substr(nm, 5, 13)
  data.frame(file = nm, tile = tile_id) %>% unique(.)
}))

# local path
dst_path <- 'C:/Users/svroc/Documents/ecaas_scripts'
dst_path_aws <- '/home/ubuntu/projects'

# read nicfi tiles geojson
tiles_nicfi <- read_sf(sprintf('%s/geoms/tiles_nicfi.geojson', dst_path))

# check if all tiles have 2 images (VV and VH)
s1l3_ctl %>% group_by(tile) %>%
  summarise(n = n())  %>% filter(n != 2)

# find unique tiles in s3
unique_tiles <- unique(s1l3_ctl$tile)

# total unique tiles
total_tiles <- unique(tiles_nicfi$tile)

# find remaining tiles
'%ni%' <- Negate("%in%")
tiles_remaining <-
  tiles_nicfi %>% filter(tiles_nicfi$tile %ni% unique_tiles)
tiles_remaining %>%  View()

# s1 footprints catalog for ejura tain
s1_footprints <- st_read(sprintf('%s/catalogs/s1_footprints_ecaas_2021_full.geojson', dst_path))

# unique s1 images
unique_s1_images <- unique(s1_footprints$title)

# identify footprints required to run harmonic for each tile
bry <- tiles_nicfi %>% filter(tile == "1008-1067")
tarea <- as.numeric(st_area(bry) / 10000)
tile_id <- bry %>% pull(tile)
ftps_sub <- st_crop(s1_footprints, bry) %>%
  select(id, title, startDate) %>%
  mutate(date = gsub("-", "", lubridate::as_date(startDate))) %>%
  arrange(date) %>% select(-startDate) %>%
  mutate(area = as.numeric(st_area(.) / 10000)) %>%
  group_by(date) %>%
  mutate(sarea = sum(area)) %>%
  filter(sarea > tarea * 0.999) %>%
  ungroup()

# unique date
dates <- unique(ftps_sub$date)

# unique images
titlelist <- unique(ftps_sub$title)

# check if there are multiple images for same day
length(titlelist) == length(dates)

# save output
save(tiles_remaining, file = file.path(dst_path, "tiles_remaining.rda"))
st_write(tiles_remaining, sprintf('%s/geoms/tiles_remaining.geojson', dst_path))

# plot the remaining tiles
ggplot() +
  geom_sf(data = s1_footprints[1:10]$geometry) +
  geom_sf(data = tiles_nicfi) +
  geom_sf(data = tiles_remaining, fill = "black")

