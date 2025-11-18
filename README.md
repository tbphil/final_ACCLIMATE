# Acclimate UI Project

This project is part of the **Acclimate** initiative, focused on visualizing climate data and infrastructure susceptibility using an interactive web interface. The application leverages modern frontend technologies including **Vue.js** and **Leaflet**, integrated with a backend using **FastAPI** to retrieve climate and infrastructure data.

## New Setup Instructions

1. **Clone the Repository**
   ```sh
   git clone https://tfs.inl.gov/tfs/HSD/ACCLIMATE/_git/ACCLIMATE
   cd acclimate
   ```
2. **Install Docker**
   Follow the [Docker Desktop] (https://docs.docker.com/desktop/setup/install/windows-install/) set up

3. **Build the Docker Images**
   To build the backend image:

```sh
cd backend
docker build -t acclimate .
```

To build the frontend image, navigate back to the main project directory:

```
cd frontend-angular
docker build -t acclimatefront .
```

4. **Start Docker Containers**
   To run the project, navigate back to the main project directory:

```
docker compose up -d --build && docker compose watch
```

## Run Locally
If you would prefer to run locally instead of on docker, navigate to the backend folder. In database.py comment out line 5 and uncomment line 4 (switch from host.docker.internal to 127.0.0.1). Then navigate to frontend-angular. In proxy.config.json line 4, change the target from http://python_web_service:8000 to http://localhost:8000.

You will need to install python, create a virtual environment, and pip install the requirements folder (you will likely need to run ```pip install pip_system_certs --trusted-host files.pythonhosted.org --trusted-host pypi.org``` unless you have already gone through the SSL Inspection Developer Tools Fix). From the frontend-angular folder, you will need to call npm install to set up the frontend environment.

Open two terminals. In the first terminal call
```
cd backend
uvicorn main:app --reload
```
In the second terminal call
``` 
npm run start
```


## Project Structure

The frontend of this project was initially set up in vue as detailed below. This can still be used, but there is now a preferred angular frontend. The backend set up has also been simplified. Follow the "New Setup Instructions" above for the preferred setup.

The following are the key components and files involved in this project:

### 1. **App.vue**

- **Role**: The main container for the application, orchestrating interactions between different components such as `DataControls`, `MapSection`, and `GraphsContainer`.
- **Key Features**:
  - Handles the primary data states (`variablesList`, `currentTimeIndex`, `infrastructureData`, etc.) that get passed down to child components.
  - Uses modern Vue bindings like `v-model` for `currentTimeIndex` to keep the components in sync.
  - Integrates with navigation and control elements to update and refresh data across the application.

### 2. **DataControls.vue**

- **Role**: Provides user controls for interacting with the data, including timeline navigation, variable selection, and toggling grid layers.
- **Key Features**:
  - Buttons for selecting variables, navigating timesteps (forward/backward), and toggling grid and infrastructure markers.
  - Emits events (`switch-variable`, `toggle-climate-grid`, etc.) to allow the parent component (`App.vue`) to react to user interactions.
  - Timeline slider to visualize and interact with specific timesteps.

### 3. **GraphsContainer.vue**

- **Role**: Visualizes climate data over time for selected variables using **Plotly**.
- **Key Features**:
  - Plots interactive graphs for each selected variable.
  - Includes a green line indicator that updates dynamically to indicate the current timestep.
  - Watches for changes in `currentTimeIndex` and updates the vertical line on the graph accordingly.

### 4. **MapSection.vue**

- **Role**: Displays geographic data on an interactive map using **Leaflet**.
- **Key Features**:
  - Shows climate grid cells and infrastructure data as overlays on the map.
  - Handles user interactions like hover events to show popups with information about specific grid cells.
  - Watches `currentTimeIndex` to update map layers to reflect the currently selected time.

### 5. **NavMenu.vue**

- **Role**: The navigation menu that allows users to input form data and control the application's configuration.
- **Key Features**:
  - Handles form submission for setting parameters like hazard type, variables, and infrastructure type.
  - Passes submitted data up to the parent (`App.vue`) to trigger updates.

## Key Concepts and Dependencies

### **Vue.js 3 Features**

- **Composition API**: This project is built using Vue 3's Composition API, allowing us to define reactive state and lifecycle hooks in a flexible way.
- **v-model**: Used to bind component props like `currentTimeIndex` to maintain reactivity between `DataControls` and `App`.

### **Plotly.js**

- Used for rendering graphs in `GraphsContainer`. Provides dynamic and responsive data visualization capabilities, ensuring users can interact with time series data effectively.

### **Leaflet and Vue-Leaflet**

- **Leaflet**: The mapping library used to visualize geographical data.
- **Vue-Leaflet**: A wrapper to facilitate using Leaflet with Vue, allowing for declarative map components like `LMap`, `LTileLayer`, and `LRectangle`.

## Installation and Setup

### Prerequisites

- **Node.js** and **npm**: Make sure you have Node.js installed, as it includes npm (Node Package Manager).
- **Python Environment**: Required for running the backend FastAPI server.

<<<<<<< HEAD

### Setup Instructions

1. **Clone the Repository**

   ```sh
   git clone https://github.com/your-repo/acclimate-ui.git
   cd acclimate-ui
   ```

2. **Install Dependencies**
   Run the following command to install the required npm packages:

   ```sh
   npm install
   ```

3. **Start the Development Server**
   Use the following command to run the application in development mode:

   ```sh
   npm run serve
   ```

   The app should be available at `http://localhost:8080/`.

4. **Backend Integration**
   - Set up the FastAPI backend for handling data requests.
   - Ensure the backend server is running and accessible by the Vue frontend.

## Common Errors and Debugging

### **Module Not Found Errors**

- Ensure that all imported components (`NavMenu.vue`, `MapSection.vue`, `GraphsContainer.vue`, etc.) exist in the correct paths.
- If you encounter `Module not found` errors, double-check the relative paths and that file names are correct.

### **Deprecated `.sync` Modifier**

- The `.sync` modifier is deprecated in Vue 3. Replace it with `v-model` with a prop name, as seen in `App.vue` where `v-model:currentTimeIndex` is used.

### **ESLint Linting Issues**

- **Unused Variables**: Make sure to remove or use any defined but unused variables (`grid`, `formData`, etc.) to avoid linting errors.
- **Run ESLint Fix**: To automatically fix linting issues, run the following command:
  ```sh
  npm run lint --fix
  ```

## Future Improvements

- **State Management**: Integrate a state management library like **Vuex** to make data sharing between components more efficient and avoid prop-drilling.
- **Backend Data Caching**: Use caching for backend data to improve load times, especially for repeated requests.
- **Testing**: Add unit and end-to-end (E2E) tests using **Jest** and **Cypress** to ensure robustness.

## Contributing

1. **Fork the Repository**: Fork the repo on GitHub.
2. **Create a Feature Branch**: Create a branch for your feature or bugfix.
   ```sh
   git checkout -b feature/my-new-feature
   ```
3. **Commit Changes**: Commit your changes with a descriptive message.
4. **Push and Create PR**: Push your changes and create a pull request for review.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.

## Acknowledgments

- **OpenStreetMap** contributors for map tiles.
- The **Vue.js** and **Leaflet** communities for their amazing open-source contributions.

=======

> > > > > > > 193de38bd07adefff3dc6297936f19a21852e5db
> > > > > > > Feel free to reach out if you have any questions or suggestions for improvement.
