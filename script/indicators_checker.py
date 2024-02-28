def calculate_yield_reduction(plant_type, climate_conditions, soil_moisture):
    reduction_percentage = 0
    
    if plant_type == "wheat":
        if climate_conditions < 4:
            reduction_percentage += 15
        elif climate_conditions < 6:
            reduction_percentage += 10
        if soil_moisture > 8:
            reduction_percentage += 7
        elif soil_moisture > 6:
            reduction_percentage += 5
    elif plant_type == "corn":
        if climate_conditions > 8:
            reduction_percentage += 20
        elif climate_conditions > 6:
            reduction_percentage += 15
        if soil_moisture < 3:
            reduction_percentage += 10
        elif soil_moisture < 5:
            reduction_percentage += 8
    elif plant_type == "potatoes":
        if climate_conditions > 7:
            reduction_percentage += 18
        elif climate_conditions > 5:
            reduction_percentage += 12
        if soil_moisture > 7:
            reduction_percentage += 10
        elif soil_moisture > 5:
            reduction_percentage += 7
    elif plant_type == "tomato":
        if climate_conditions > 6:
            reduction_percentage += 16
        elif climate_conditions > 4:
            reduction_percentage += 14
        if soil_moisture > 6:
            reduction_percentage += 8
        elif soil_moisture > 4:
            reduction_percentage += 6
    elif plant_type == "cucumber":
        if climate_conditions > 7:
            reduction_percentage += 15
        elif climate_conditions > 5:
            reduction_percentage += 13
        if soil_moisture > 7:
            reduction_percentage += 8
        elif soil_moisture > 5:
            reduction_percentage += 5
    elif plant_type == "greenbeans":
        if climate_conditions > 7:
            reduction_percentage += 13
        elif climate_conditions > 5:
            reduction_percentage += 11
        if soil_moisture > 6:
            reduction_percentage += 6
        elif soil_moisture > 4:
            reduction_percentage += 4
    
    return reduction_percentage
